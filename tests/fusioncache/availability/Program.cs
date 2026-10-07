using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Backplane;
using ZiggyCreatures.Caching.Fusion.Locking.Distributed;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

internal static class Program
{
    private static readonly List<object> Results = [];
    private static void Check(bool condition, string description)
    {
        if (!condition) throw new InvalidOperationException(description);
    }
    private static FusionCache Node(string id, Backend backend, Notifications notifications, Locker? locker = null)
    {
        var cache = new FusionCache(new FusionCacheOptions
        {
            CacheName = id,
            CacheKeyPrefix = id + ":",
            EnableAutoRecovery = false,
            EnableSyncEventHandlersExecution = true,
            DefaultEntryOptions = new FusionCacheEntryOptions(TimeSpan.FromMinutes(1))
            {
                AllowBackgroundDistributedCacheOperations = false,
                AllowBackgroundBackplaneOperations = false,
            },
        });
        cache.SetupDistributedCache(backend, new FusionCacheSystemTextJsonSerializer());
        cache.SetupBackplane(notifications);
        if (locker is not null) cache.SetupDistributedLocker(locker);
        return cache;
    }
    private static async Task HotAcrossDisconnectAndReconnect(bool withLocker)
    {
        var backend = new Backend();
        var notifications = new Notifications();
        var locker = withLocker ? new Locker() : null;
        using var cache = Node("hot-" + withLocker, backend, notifications, locker);
        await cache.SetAsync("key", 42);
        backend.Down = notifications.Down = true;
        if (locker is not null) locker.Down = true;
        // A failed publication exposes the unavailable provider to the core.
        await cache.SetAsync("probe", 1);
        var calls = 0;
        var during = await cache.GetOrSetAsync<int>("key", (_, _) => { calls++; return Task.FromResult(7); });
        Check(during == 42 && calls == 0, "warm L1 retained over backplane publication failure");
        backend.Down = notifications.Down = false;
        if (locker is not null) locker.Down = false;
        backend.Clear(); // Simulate a removal that was missed while unsubscribed.
        await notifications.Reconnect();
        var after = await cache.GetOrSetAsync<int>("key", (_, _) => { calls++; return Task.FromResult(7); });
        Check(after == 42 && calls == 0, "reconnect does not discard L1 after a missed removal");
        Check(locker is null || locker.Acquires == 0, "hot service does not acquire a distributed lock");
        Results.Add(new { kind = "hot-gap-reconnect", withLocker, during, after, factoryCalls = calls,
            lockerCalls = locker?.Acquires ?? 0, missedRemovalVisible = false });
    }
    private static async Task ColdLockerFailure(bool rethrow)
    {
        var backend = new Backend { Down = true };
        var notifications = new Notifications();
        var locker = new Locker { Down = true };
        using var cache = Node("cold-" + rethrow, backend, notifications, locker);
        var opts = cache.DefaultEntryOptions.Duplicate();
        opts.ReThrowDistributedLockerExceptions = rethrow;
        var calls = 0;
        string? error = null;
        int? value = null;
        try
        {
            value = await cache.GetOrSetAsync<int>("key", (_, _) => { calls++; return Task.FromResult(7); }, opts);
        }
        catch (FusionCacheDistributedLockerException e) { error = e.GetType().Name; }
        Check(rethrow ? error is not null && calls == 0 : value == 7 && calls == 1,
            "ordinary locker error propagation follows the explicit flag");
        Results.Add(new { kind = "cold-locker-failure", rethrow, value, error, factoryCalls = calls });
    }
    private static async Task StaleAcrossGap()
    {
        var backend = new Backend();
        var notifications = new Notifications();
        var locker = new Locker();
        using var cache = Node("stale", backend, notifications, locker);
        var opts = cache.DefaultEntryOptions.Duplicate();
        opts.Duration = TimeSpan.FromMilliseconds(100);
        opts.IsFailSafeEnabled = true;
        opts.FailSafeMaxDuration = TimeSpan.FromMinutes(1);
        await cache.SetAsync("key", 42, opts);
        await Task.Delay(160);
        backend.Down = notifications.Down = locker.Down = true;
        await cache.SetAsync("probe", 1);
        var calls = 0;
        var value = await cache.GetOrSetAsync<int>("key", (_, _) =>
        {
            calls++;
            return Task.FromException<int>(new InvalidOperationException("origin unavailable"));
        }, opts);
        Check(value == 42 && calls == 1, "retained stale survives the unavailable backplane and locker");
        Results.Add(new { kind = "stale-gap-fail-safe", value, factoryCalls = calls });
    }
    private static FusionCache LocalWithL2(Backend backend, FusionCacheEntryOptions options)
    {
        var cache = new FusionCache(new FusionCacheOptions
        {
            EnableAutoRecovery = false,
            DefaultEntryOptions = options,
        });
        cache.SetupDistributedCache(backend, new FusionCacheSystemTextJsonSerializer());
        return cache;
    }
    private static async Task AvailabilityDefaults()
    {
        var defaults = new FusionCacheOptions();
        Check(!defaults.WaitForInitialBackplaneSubscribe, "default startup does not wait for subscription");
        Check(defaults.AutoRecoveryDelay == TimeSpan.FromSeconds(5), "five-second default recovery delay");
        Results.Add(new { kind = "availability-defaults", initialSubscriptionWait = false, recoveryDelaySeconds = 5 });
        var backend = new Backend();
        var options = new FusionCacheEntryOptions(TimeSpan.FromMinutes(1));
        using (var cache = LocalWithL2(backend, options))
        {
            await cache.SetAsync("hot", 42);
            Check(await cache.GetOrSetAsync<int>("hot", (_, _) => Task.FromResult(-1)) == 42, "prime the read-side marker observations");
            var reads = backend.Reads;
            await Task.Delay(2100);
            var calls = 0;
            var value = await cache.GetOrSetAsync<int>("hot", (_, _) => { calls++; return Task.FromResult(7); });
            Check(value == 42 && calls == 0 && backend.Reads == reads, $"L2-only L1 survives multiple seconds without periodic clearing: value={value}, factoryCalls={calls}, extraReads={backend.Reads - reads}");
            Results.Add(new { kind = "l2-only-no-periodic-clear", value, factoryCalls = calls, extraDistributedReads = backend.Reads - reads });
        }
        backend = new Backend();
        options = new FusionCacheEntryOptions(TimeSpan.FromMinutes(1)) { IsFailSafeEnabled = true };
        using (var cache = LocalWithL2(backend, options))
        {
            await cache.SetAsync("hot", 42);
            await cache.ExpireAsync("hot");
            Check(backend.Removes == 1 && backend.Count == 0, "expire removes L2");
            var value = await cache.GetOrSetAsync<int>("hot", (_, _) => Task.FromException<int>(new IOException("origin unavailable")));
            Check(value == 42, "expire keeps eligible L1 fail-safe");
            Results.Add(new { kind = "expire-removes-l2", value, distributedRemoves = backend.Removes, distributedValues = backend.Count });
        }
        using (var cache = new FusionCache(new FusionCacheOptions { EnableAutoRecovery = false }))
        {
            options = new FusionCacheEntryOptions(TimeSpan.FromMinutes(1))
            {
                IsFailSafeEnabled = true,
                FactorySoftTimeout = TimeSpan.FromMilliseconds(5),
                FactoryHardTimeout = TimeSpan.FromSeconds(1),
            };
            var value = await cache.GetOrSetAsync<int>("cold", async (_, token) =>
            {
                await Task.Delay(100, token);
                return 7;
            }, 99, options);
            Check(value == 7, "fail-safe default without stale does not activate soft timeout");
            Results.Add(new { kind = "soft-timeout-requires-stale", failSafeDefault = 99, value });
        }
    }
    public static async Task Main()
    {
        var version = typeof(FusionCache).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion;
        Check(version == "2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd", "exact released binary pin");
        Results.Add(new { kind = "pins", version, assembly = typeof(FusionCache).Assembly.Location,
            seam = "public provider interfaces; controlled transport failure and reconnect callback", nativeRedis = false });
        await AvailabilityDefaults();
        await HotAcrossDisconnectAndReconnect(false);
        await HotAcrossDisconnectAndReconnect(true);
        await ColdLockerFailure(false);
        await ColdLockerFailure(true);
        await StaleAcrossGap();
        Results.Add(new { kind = "summary", scenarios = 9, assertions = "all passed" });
        Console.WriteLine(JsonSerializer.Serialize(Results, new JsonSerializerOptions { WriteIndented = true }));
    }
}
internal sealed class Backend : IDistributedCache
{
    private readonly ConcurrentDictionary<string, byte[]> _values = new();
    private int reads;
    private int removes;
    public int Reads => Volatile.Read(ref reads);
    public int Removes => Volatile.Read(ref removes);
    public int Count => _values.Count;
    public bool Down { get; set; }
    public void Clear() => _values.Clear();
    private void Available() { if (Down) throw new IOException("byte store unavailable"); }
    public byte[]? Get(string key) { Available(); Interlocked.Increment(ref reads); return _values.TryGetValue(key, out var value) ? value : null; }
    public Task<byte[]?> GetAsync(string key, CancellationToken token = default) => Task.FromResult(Get(key));
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) { Available(); _values[key] = value; }
    public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
    { Set(key, value, options); return Task.CompletedTask; }
    public void Refresh(string key) => Available();
    public Task RefreshAsync(string key, CancellationToken token = default) { Refresh(key); return Task.CompletedTask; }
    public void Remove(string key) { Available(); Interlocked.Increment(ref removes); _values.TryRemove(key, out _); }
    public Task RemoveAsync(string key, CancellationToken token = default) { Remove(key); return Task.CompletedTask; }
}
internal sealed class Notifications : IFusionCacheBackplane
{
    private BackplaneSubscriptionOptions? _subscription;
    public bool Down { get; set; }
    public void Subscribe(BackplaneSubscriptionOptions options)
    { _subscription = options; options.ConnectHandler?.Invoke(new BackplaneConnectionInfo(false)); }
    public async ValueTask SubscribeAsync(BackplaneSubscriptionOptions options)
    { _subscription = options; if (options.ConnectHandlerAsync is { } handler) await handler(new BackplaneConnectionInfo(false)); }
    public void Unsubscribe() => _subscription = null;
    public ValueTask UnsubscribeAsync() { Unsubscribe(); return ValueTask.CompletedTask; }
    public void Publish(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
    { token.ThrowIfCancellationRequested(); if (Down) throw new IOException("backplane unavailable"); }
    public ValueTask PublishAsync(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
    { Publish(message, options, token); return ValueTask.CompletedTask; }
    public async ValueTask Reconnect()
    { if (_subscription?.ConnectHandlerAsync is { } handler) await handler(new BackplaneConnectionInfo(true)); }
}
internal sealed class Locker : IFusionCacheDistributedLocker
{
    public bool Down { get; set; }
    public int Acquires { get; private set; }
    public object? AcquireLock(string cacheName, string cacheInstanceId, string operationId, string key,
        string lockName, TimeSpan timeout, ILogger? logger, CancellationToken token)
    { token.ThrowIfCancellationRequested(); Acquires++; if (Down) throw new IOException("locker unavailable"); return new object(); }
    public ValueTask<object?> AcquireLockAsync(string cacheName, string cacheInstanceId, string operationId, string key,
        string lockName, TimeSpan timeout, ILogger? logger, CancellationToken token)
        => ValueTask.FromResult(AcquireLock(cacheName, cacheInstanceId, operationId, key, lockName, timeout, logger, token));
    public void ReleaseLock(string cacheName, string cacheInstanceId, string operationId, string key,
        string lockName, object? lockObj, ILogger? logger, CancellationToken token) { }
    public ValueTask ReleaseLockAsync(string cacheName, string cacheInstanceId, string operationId, string key,
        string lockName, object? lockObj, ILogger? logger, CancellationToken token) => ValueTask.CompletedTask;
}
