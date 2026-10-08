using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Internals.Distributed;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

internal static class Program
{
    private static readonly List<object> Results = [];
    private static readonly FusionCacheSystemTextJsonSerializer Serializer = new();
    private static void Check(bool condition, string description)
    {
        if (!condition) throw new InvalidOperationException(description);
    }
    private static FusionCache Node(RecordingStore store, string prefix = "", bool removeTags = false,
        FusionCacheEntryOptions? tagOptions = null, bool recovery = false)
    {
        var cache = new FusionCache(new FusionCacheOptions
        {
            CacheKeyPrefix = prefix,
            EnableAutoRecovery = recovery,
            AutoRecoveryDelay = TimeSpan.FromMilliseconds(30),
            DistributedCacheCircuitBreakerDuration = TimeSpan.Zero,
            RemoveByTagBehavior = removeTags ? RemoveByTagBehavior.Remove : RemoveByTagBehavior.Expire,
            TagsDefaultEntryOptions = tagOptions ?? new FusionCacheOptions().TagsDefaultEntryOptions,
            DefaultEntryOptions = new FusionCacheEntryOptions(TimeSpan.FromMinutes(1))
            {
                IsFailSafeEnabled = true,
                FailSafeMaxDuration = TimeSpan.FromMinutes(2),
                FailSafeThrottleDuration = TimeSpan.FromSeconds(1),
                AllowBackgroundDistributedCacheOperations = false,
                AllowBackgroundBackplaneOperations = false,
            },
        });
        cache.SetupDistributedCache(store, Serializer);
        return cache;
    }
    private static async Task FirstHotRead()
    {
        var store = new RecordingStore();
        using var cache = Node(store);
        await cache.SetAsync("hot", 42);
        var before = store.Reads;
        var calls = 0;
        var first = await cache.GetOrSetAsync<int>("hot", (_, _) => { calls++; return Task.FromResult(7); });
        var afterFirst = store.Reads;
        for (var i = 0; i < 3; i++)
            Check(await cache.GetOrSetAsync<int>("hot", (_, _) => { calls++; return Task.FromResult(7); }) == 42, "retained warm L1");
        Check(first == 42 && calls == 0, "first hot read retains L1 without the factory");
        Check(afterFirst - before == 2, "first untagged read checks two clear markers");
        Check(store.Reads == afterFirst, "subsequent hot reads reuse initialized marker observations");
        Results.Add(new { kind = "first-hot-read", value = first, factoryCalls = calls,
            initialMarkerReads = afterFirst - before, subsequentReads = store.Reads - afterFirst });
    }
    private static async Task ColdTag()
    {
        var store = new RecordingStore();
        using var writer = Node(store, "scope:");
        await writer.SetAsync("entry", 42, tags: ["group"]);
        var data = store.Writes.Last();
        var original = store.Get(data.Key)!;
        await Task.Delay(5);
        await writer.RemoveByTagAsync("group");
        var marker = store.Writes.Last();
        Check(marker.Key != data.Key && store.Get(data.Key)!.SequenceEqual(original), "tag writes a separate marker without removing value bytes");
        Check(store.Removes == 0, "tag does not physically remove the value");
        Check(marker.Ttl <= TimeSpan.FromDays(10) && marker.Ttl >= TimeSpan.FromDays(10) - TimeSpan.FromSeconds(1),
            $"default marker physical TTL includes fail-safe retention: {marker.Ttl}");
        using var cold = Node(store, "scope:");
        var calls = 0;
        var value = await cold.GetOrSetAsync<int>("entry", (_, _) => { calls++; return Task.FromResult(7); });
        Check(value == 7 && calls == 1, "cold node rejects the invalidated value");
        Results.Add(new { kind = "cold-tag", value, factoryCalls = calls, valueBytesRetained = true,
            markerTtlSeconds = marker.Ttl!.Value.TotalSeconds, removes = store.Removes });
    }
    private static async Task FailSafe(bool clear, bool remove)
    {
        var store = new RecordingStore();
        using var writer = Node(store, removeTags: remove);
        await writer.SetAsync("entry", 42, tags: ["group"]);
        await Task.Delay(5);
        if (clear) await writer.ClearAsync(allowFailSafe: !remove);
        else await writer.RemoveByTagAsync("group");
        using var cold = Node(store, removeTags: remove);
        var calls = 0;
        int? value = null;
        string? error = null;
        try
        {
            value = await cold.GetOrSetAsync<int>("entry", (_, _) =>
            {
                calls++;
                return Task.FromException<int>(new IOException("origin unavailable"));
            });
        }
        catch (IOException ex) { error = ex.GetType().Name; }
        Check(calls == 1, "invalidation invokes the origin before considering fail-safe");
        Check(remove ? value is null && error is not null : value == 42 && error is null,
            "Expire preserves stale while Remove rejects stale");
        Results.Add(new { kind = "invalidation-fail-safe", clear, remove, value, error, factoryCalls = calls });
    }
    private static async Task PrefixIsolation()
    {
        var store = new RecordingStore();
        using var a = Node(store, "a:");
        using var b = Node(store, "b:");
        await a.SetAsync("entry", 42, tags: ["group"]);
        await b.SetAsync("entry", 42, tags: ["group"]);
        await Task.Delay(5);
        await a.RemoveByTagAsync("group");
        using var cold = Node(store, "b:");
        var calls = 0;
        var value = await cold.GetOrSetAsync<int>("entry", (_, _) => { calls++; return Task.FromResult(7); });
        Check(value == 42 && calls == 0, "a marker cannot invalidate another key-prefix scope");
        Results.Add(new { kind = "prefix-isolation", value, factoryCalls = calls });
    }
    private static async Task ReloadRevision()
    {
        var store = new RecordingStore();
        using var writer = Node(store);
        await writer.SetAsync("entry", 42, tags: ["group"]);
        await Task.Delay(5);
        await writer.RemoveByTagAsync("group");
        var marker = store.Writes.Last();
        var original = store.Get(marker.Key)!;
        var originalRevision = Serializer.Deserialize<FusionCacheDistributedEntry<long>>(original)!.Value;
        var tagOptions = new FusionCacheOptions().TagsDefaultEntryOptions.Duplicate();
        tagOptions.MemoryCacheDuration = TimeSpan.FromMilliseconds(10);
        using var cold = Node(store, tagOptions: tagOptions);
        for (var i = 0; i < 3; i++)
        {
            Check(!(await cold.TryGetAsync<int>("entry")).HasValue, "invalidated snapshot remains unavailable after marker observation expires");
            await Task.Delay(20);
        }
        var after = store.Get(marker.Key)!;
        Check(after.SequenceEqual(original), "re-observation does not rewrite the remote marker");
        Check(Serializer.Deserialize<FusionCacheDistributedEntry<long>>(after)!.Value == originalRevision,
            "marker retains the original invalidation timestamp");
        Check(store.Writes.Count(write => write.Key == marker.Key) == 1, "no marker renewal on successful remote observation");
        Results.Add(new { kind = "reload-original-revision", rewrites = 0, originalRevision });
    }
    private static async Task Boundary(MarkerKind kind, long difference)
    {
        var store = new RecordingStore();
        using (var writer = Node(store, removeTags: true))
        {
            await writer.SetAsync("entry", 42L, tags: kind == MarkerKind.Tag ? ["group"] : []);
            var dataKey = store.Writes.Last().Key;
            var data = Serializer.Deserialize<FusionCacheDistributedEntry<long>>(store.Get(dataKey)!)!;
            switch (kind)
            {
                case MarkerKind.Tag: await writer.RemoveByTagAsync("group"); break;
                case MarkerKind.ClearExpire: await writer.ClearAsync(true); break;
                case MarkerKind.ClearRemove: await writer.ClearAsync(false); break;
                default: throw new InvalidOperationException("unexpected closed marker kind");
            }
            var markerKey = store.Writes.Last().Key;
            Check(markerKey != dataKey, "separate marker record");
            var marker = Serializer.Deserialize<FusionCacheDistributedEntry<long>>(store.Get(markerKey)!)!;
            data.Timestamp = checked(marker.Value + difference);
            store.Set(dataKey, Serializer.Serialize(data), new DistributedCacheEntryOptions
                { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) });
        }
        using var cold = Node(store, removeTags: true);
        var value = await cold.TryGetAsync<long>("entry");
        Check(value.HasValue == (difference > 0) && (!value.HasValue || value.Value == 42), "inclusive invalidation timestamp boundary");
        Results.Add(new { kind = "marker-boundary", marker = kind.ToString(), createdMinusMarker = difference,
            present = value.HasValue });
    }
    private static async Task RecoveryLifetime()
    {
        var store = new RecordingStore();
        var tagOptions = new FusionCacheOptions().TagsDefaultEntryOptions.Duplicate();
        tagOptions.IsFailSafeEnabled = false;
        tagOptions.DistributedCacheDuration = TimeSpan.FromSeconds(5);
        using var cache = Node(store, tagOptions: tagOptions, recovery: true);
        store.Down = true;
        await cache.RemoveByTagAsync("group");
        var original = store.Writes.First();
        await Task.Delay(200);
        store.Down = false;
        var stop = DateTimeOffset.UtcNow.AddSeconds(3);
        while (store.Writes.Count < 2 || store.Get(original.Key) is null)
        {
            Check(DateTimeOffset.UtcNow < stop, "recovery made the marker visible before the deadline");
            await Task.Delay(5);
        }
        var recovered = store.Writes.Last();
        Check(recovered.Bytes.SequenceEqual(original.Bytes), "recovery preserves the serialized marker revision");
        Check(recovered.ExpiresAt > original.ExpiresAt, "FC recomputes the physical marker expiry on replay");
        Results.Add(new { kind = "recovery-lifetime", revisionRetained = true,
            originalTtlSeconds = original.Ttl!.Value.TotalSeconds,
            recoveredTtlSeconds = recovered.Ttl!.Value.TotalSeconds,
            originalExpiresAt = original.ExpiresAt, recoveredExpiresAt = recovered.ExpiresAt,
            physicalDeadlineRetained = recovered.ExpiresAt == original.ExpiresAt });
    }
    public static async Task Main()
    {
        var assembly = typeof(FusionCache).Assembly;
        var version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion;
        Check(version == "2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd", "exact released package pin");
        Results.Add(new { kind = "pins", version, seam = "public cache and byte-provider interfaces; serialized timestamp boundary fixture", nativeRedis = false });
        await FirstHotRead();
        await ColdTag();
        foreach (var clear in new[] { false, true })
            foreach (var remove in new[] { false, true }) await FailSafe(clear, remove);
        await PrefixIsolation();
        await ReloadRevision();
        foreach (var kind in Enum.GetValues<MarkerKind>())
            foreach (var difference in new[] { -1L, 0L, 1L }) await Boundary(kind, difference);
        await RecoveryLifetime();
        Results.Add(new { kind = "summary", scenarios = Results.Count - 1, assertions = "all passed" });
        Console.WriteLine(JsonSerializer.Serialize(Results, new JsonSerializerOptions { WriteIndented = true }));
    }
}
internal enum MarkerKind { Tag, ClearExpire, ClearRemove }
internal sealed record Write(string Key, byte[] Bytes, TimeSpan? Ttl, DateTimeOffset? ExpiresAt);
internal sealed class RecordingStore : IDistributedCache
{
    private readonly MemoryDistributedCache inner = new(Options.Create(new MemoryDistributedCacheOptions()));
    private int reads;
    private int removes;
    private int down;
    public ConcurrentQueue<Write> Writes { get; } = new();
    public int Reads => Volatile.Read(ref reads);
    public int Removes => Volatile.Read(ref removes);
    public bool Down { get => Volatile.Read(ref down) != 0; set => Volatile.Write(ref down, value ? 1 : 0); }
    private void Available() { if (Down) throw new IOException("byte store unavailable"); }
    public byte[]? Get(string key) { Available(); Interlocked.Increment(ref reads); return inner.Get(key); }
    public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    { token.ThrowIfCancellationRequested(); return Task.FromResult(Get(key)); }
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        var now = DateTimeOffset.UtcNow;
        var expiry = options.AbsoluteExpiration ?? (options.AbsoluteExpirationRelativeToNow is { } relative ? now + relative : (DateTimeOffset?)null);
        Writes.Enqueue(new Write(key, value.ToArray(), expiry - now, expiry));
        Available(); inner.Set(key, value, options);
    }
    public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
    { token.ThrowIfCancellationRequested(); Set(key, value, options); return Task.CompletedTask; }
    public void Remove(string key) { Available(); Interlocked.Increment(ref removes); inner.Remove(key); }
    public Task RemoveAsync(string key, CancellationToken token = default)
    { token.ThrowIfCancellationRequested(); Remove(key); return Task.CompletedTask; }
    public void Refresh(string key) { Available(); inner.Refresh(key); }
    public Task RefreshAsync(string key, CancellationToken token = default)
    { token.ThrowIfCancellationRequested(); Refresh(key); return Task.CompletedTask; }
}
