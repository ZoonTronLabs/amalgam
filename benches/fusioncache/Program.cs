using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using ZiggyCreatures.Caching.Fusion;

internal static class Program
{
    private const int Operations = 300_000;
    private const int SetOperations = 1_000_000;
    private const int ColdOperations = 100_000;
    private static FusionCache New() => new(new FusionCacheOptions
    {
        DefaultEntryOptions = new(TimeSpan.FromHours(1)),
        EnableAutoRecovery = false,
    });
    private interface IWarmHit
    {
        static abstract long AsyncValue(FusionCache cache, string key);
        static abstract long Sync(FusionCache cache, string key);
    }
    private readonly struct ReadHit : IWarmHit
    {
        public static long AsyncValue(FusionCache cache, string key) => cache.TryGetAsync<long>(key).GetAwaiter().GetResult().Value;
        public static long Sync(FusionCache cache, string key) => cache.TryGet<long>(key).Value;
    }
    private readonly struct OriginHit : IWarmHit
    {
        public static long AsyncValue(FusionCache cache, string key) => cache.GetOrSetAsync<long>(key, static (_, _) => Task.FromException<long>(new InvalidOperationException("A warmed factory must never run"))).GetAwaiter().GetResult();
        public static long Sync(FusionCache cache, string key) => cache.GetOrSet<long>(key, static (_, _) => throw new InvalidOperationException("A warmed native factory must never run"));
    }
    private static void Scaling<H>(FusionCache cache, string[] keys, int workers, bool same) where H : struct, IWarmHit
    {
        using Barrier gate = new(workers + 1);
        Thread[] threads = new Thread[workers];
        long[] allocations = new long[workers];
        long[] sums = new long[workers];
        for (int id = 0; id < workers; id++)
        {
            int worker = id;
            int index = same ? 0 : id;
            string key = keys[index];
            threads[id] = new Thread(() =>
            {
                string workerLabel = same ? "same" : "distinct";
                SteadyWarmup warmup = new($"{workerLabel}:{workers}:{worker}");
                while (true)
                {
                    long batch = Stopwatch.GetTimestamp();
                    for (int n = 0; n < SteadyWarmup.Batch; n++)
                        if (H.AsyncValue(cache, key) != index + 1)
                            throw new InvalidOperationException("Warm value mismatch");
                    if (warmup.Record(Stopwatch.GetElapsedTime(batch), SteadyWarmup.Batch)) break;
                }
                gate.SignalAndWait();
                gate.SignalAndWait();
                long before = GC.GetAllocatedBytesForCurrentThread();
                long sum = 0;
                for (int n = 0; n < Operations; n++)
                    sum += H.AsyncValue(cache, key);
                allocations[worker] = GC.GetAllocatedBytesForCurrentThread() - before;
                sums[worker] = sum;
            });
            threads[id].Start();
        }
        gate.SignalAndWait();
        long began = Stopwatch.GetTimestamp();
        gate.SignalAndWait();
        foreach (Thread thread in threads) thread.Join();
        double elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        for (int id = 0; id < workers; id++)
            if (sums[id] != (long)Operations * (same ? 1 : id + 1))
                throw new InvalidOperationException("Parallel checksum mismatch");
        string label = same ? "same" : "distinct";
        Console.WriteLine(FormattableString.Invariant($"{label},{workers},{workers * Operations},{elapsed / (workers * Operations):F3},{allocations.Sum()}"));
    }
    private static void SynchronousHit<H>() where H : struct, IWarmHit
    {
        using FusionCache cache = New();
        cache.Set("sync", 1L);
        SteadyWarmup warmup = new("sync");
        while (true)
        {
            long batch = Stopwatch.GetTimestamp();
            for (int n = 0; n < SteadyWarmup.Batch; n++)
                if (H.Sync(cache, "sync") != 1) throw new InvalidOperationException("Sync warmup mismatch");
            if (warmup.Record(Stopwatch.GetElapsedTime(batch), SteadyWarmup.Batch)) break;
        }
        long before = GC.GetAllocatedBytesForCurrentThread();
        long began = Stopwatch.GetTimestamp();
        long sum = 0;
        for (int n = 0; n < Operations; n++) sum += H.Sync(cache, "sync");
        double elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        if (sum != Operations) throw new InvalidOperationException("Sync checksum mismatch");
        Console.WriteLine(FormattableString.Invariant($"sync,1,{Operations},{elapsed / Operations:F3},{GC.GetAllocatedBytesForCurrentThread() - before}"));
    }
    public static Task Main(string[] args) => args switch
    {
        ["--mutations"] => RunMutations(),
        ["--l2", "--api", "read"] => RunDistributed<ReadHit>(),
        ["--l2", "--api", "get-or-set"] => RunDistributed<OriginHit>(),
        [] or ["--api", "read"] => Run<ReadHit>(),
        ["--api", "get-or-set"] => Run<OriginHit>(),
        _ => throw new ArgumentException("Expected --api read|get-or-set or --mutations"),
    };
    private static async Task Run<H>() where H : struct, IWarmHit
    {
        Identity();
        using FusionCache cache = New();
        string[] keys = Enumerable.Range(0, 8).Select(id => $"key-{id}").ToArray();
        for (int id = 0; id < keys.Length; id++) await cache.SetAsync(keys[id], (long)id + 1);
        Console.WriteLine("scenario,threads,operations,ns_per_op,allocated_bytes");
        foreach (int workers in new[] { 1, 2, 4, 8 })
            foreach (bool same in new[] { true, false }) Scaling<H>(cache, keys, workers, same);
        SynchronousHit<H>();
    }
    private static void Identity()
    {
        Assembly library = typeof(FusionCache).Assembly;
        Console.Error.WriteLine(library.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion);
        Console.Error.WriteLine(Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(library.Location))).ToLowerInvariant());
        Console.Error.WriteLine(System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);
        // FusionCache key checks are culture-sensitive; the harness pins the culture.
        string culture = System.Globalization.CultureInfo.CurrentCulture.Name;
        Console.Error.WriteLine("culture=" + (culture.Length == 0 ? "invariant" : culture));
    }
    private static async Task RunMutations()
    {
        Identity();
        Console.WriteLine("scenario,threads,operations,ns_per_op,allocated_bytes");
        await Mutations();
    }
    private static async Task Mutations()
    {
        using FusionCache writes = New();
        SteadyWarmup warmup = new("set");
        while (true)
        {
            long batch = Stopwatch.GetTimestamp();
            for (int id = 0; id < SteadyWarmup.Batch; id++) await writes.SetAsync("replace", (long)id);
            if (warmup.Record(Stopwatch.GetElapsedTime(batch), SteadyWarmup.Batch)) break;
        }
        long before = GC.GetAllocatedBytesForCurrentThread();
        long began = Stopwatch.GetTimestamp();
        for (int id = 1; id <= SetOperations; id++) await writes.SetAsync("replace", (long)id);
        double elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        Console.WriteLine(FormattableString.Invariant($"set,1,{SetOperations},{elapsed / SetOperations:F3},{GC.GetAllocatedBytesForCurrentThread() - before}"));
        if ((await writes.TryGetAsync<long>("replace")).Value != SetOperations) throw new InvalidOperationException("Replacement mismatch");
        string[] warmKeys = Enumerable.Range(0, SteadyWarmup.Batch).Select(id => $"warm-{id}").ToArray();
        warmup = new("cold");
        while (true)
        {
            TimeSpan duration;
            using (FusionCache warm = New())
            {
                long batch = Stopwatch.GetTimestamp();
                foreach (string key in warmKeys)
                    if (await warm.GetOrSetAsync<long>(key, static (_, _) => Task.FromResult(7L)) != 7)
                        throw new InvalidOperationException("Factory warmup mismatch");
                duration = Stopwatch.GetElapsedTime(batch);
            }
            if (warmup.Record(duration, SteadyWarmup.Batch)) break;
        }
        string[] cold = Enumerable.Range(0, ColdOperations).Select(id => $"cold-{id}").ToArray();
        before = GC.GetAllocatedBytesForCurrentThread();
        began = Stopwatch.GetTimestamp();
        foreach (string key in cold)
            if (await writes.GetOrSetAsync<long>(key, static (_, _) => Task.FromResult(7L)) != 7)
                throw new InvalidOperationException("Cold value mismatch");
        elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        Console.WriteLine(FormattableString.Invariant($"cold,1,{ColdOperations},{elapsed / ColdOperations:F3},{GC.GetAllocatedBytesForCurrentThread() - before}"));
    }
    private static FusionCache NewDistributed()
    {
        var cache = new FusionCache(new FusionCacheOptions
        {
            DefaultEntryOptions = new(TimeSpan.FromHours(1))
            {
                SkipMemoryCacheRead = true,
            },
            EnableAutoRecovery = false,
        });
        var distributed = new Microsoft.Extensions.Caching.Distributed.MemoryDistributedCache(
            Microsoft.Extensions.Options.Options.Create(
                new Microsoft.Extensions.Caching.Memory.MemoryDistributedCacheOptions()));
        cache.SetupDistributedCache(distributed,
            new ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson.FusionCacheSystemTextJsonSerializer());
        return cache;
    }
    private static async Task RunDistributed<H>() where H : struct, IWarmHit
    {
        Identity();
        using FusionCache cache = NewDistributed();
        await cache.SetAsync("l2-json", 7L);
        var localOnly = new FusionCacheEntryOptions(TimeSpan.FromHours(1))
        {
            SkipMemoryCacheRead = true,
            SkipDistributedCacheWrite = true,
        };
        await cache.SetAsync("l2-json", 11L, localOnly);
        SteadyWarmup warmup = new("l2_json");
        while (true)
        {
            long batch = Stopwatch.GetTimestamp();
            for (int n = 0; n < SteadyWarmup.Batch; n++)
                if (H.AsyncValue(cache, "l2-json") != 7)
                    throw new InvalidOperationException("Distributed warmup reused the wrong L1 value");
            if (warmup.Record(Stopwatch.GetElapsedTime(batch), SteadyWarmup.Batch)) break;
        }
        long before = GC.GetAllocatedBytesForCurrentThread();
        long began = Stopwatch.GetTimestamp();
        long checksum = 0;
        for (int n = 0; n < Operations; n++) checksum += H.AsyncValue(cache, "l2-json");
        double elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        if (checksum != 7L * Operations)
            throw new InvalidOperationException("Distributed checksum mismatch");
        Console.WriteLine("scenario,threads,operations,ns_per_op,allocated_bytes");
        Console.WriteLine(FormattableString.Invariant($"l2_json,1,{Operations},{elapsed / Operations:F3},{allocated}"));
    }
}
