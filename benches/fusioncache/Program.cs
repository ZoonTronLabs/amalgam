using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using ZiggyCreatures.Caching.Fusion;

internal static class Program
{
    private const int Operations = 300_000;
    private const int Warmup = 20_000;
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
                for (int n = 0; n < Warmup; n++)
                    if (H.AsyncValue(cache, key) != index + 1)
                        throw new InvalidOperationException("Warm value mismatch");
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
        for (int n = 0; n < Warmup; n++)
            if (H.Sync(cache, "sync") != 1) throw new InvalidOperationException("Sync warmup mismatch");
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
        [] or ["--api", "read"] => Run<ReadHit>(),
        ["--api", "get-or-set"] => Run<OriginHit>(),
        _ => throw new ArgumentException("Expected --api read|get-or-set"),
    };
    private static async Task Run<H>() where H : struct, IWarmHit
    {
        Assembly library = typeof(FusionCache).Assembly;
        Console.Error.WriteLine(library.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion);
        Console.Error.WriteLine(Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(library.Location))).ToLowerInvariant());
        Console.Error.WriteLine(System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);
        using FusionCache cache = New();
        string[] keys = Enumerable.Range(0, 8).Select(id => $"key-{id}").ToArray();
        for (int id = 0; id < keys.Length; id++) await cache.SetAsync(keys[id], (long)id + 1);
        Console.WriteLine("scenario,threads,operations,ns_per_op,allocated_bytes");
        foreach (int workers in new[] { 1, 2, 4, 8 })
            foreach (bool same in new[] { true, false }) Scaling<H>(cache, keys, workers, same);
        SynchronousHit<H>();
        using FusionCache writes = New();
        for (int id = 0; id < Warmup; id++) await writes.SetAsync("replace", (long)id);
        long before = GC.GetAllocatedBytesForCurrentThread();
        long began = Stopwatch.GetTimestamp();
        for (int id = 1; id <= Warmup; id++) await writes.SetAsync("replace", (long)id);
        double elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        Console.WriteLine(FormattableString.Invariant($"set,1,{Warmup},{elapsed / Warmup:F3},{GC.GetAllocatedBytesForCurrentThread() - before}"));
        if ((await writes.TryGetAsync<long>("replace")).Value != Warmup) throw new InvalidOperationException("Replacement mismatch");
        using (FusionCache warm = New())
            for (int id = 0; id < Warmup; id++)
                if (await warm.GetOrSetAsync<long>($"warm-{id}", static (_, _) => Task.FromResult(7L)) != 7)
                    throw new InvalidOperationException("Factory warmup mismatch");
        string[] cold = Enumerable.Range(0, Warmup).Select(id => $"cold-{id}").ToArray();
        before = GC.GetAllocatedBytesForCurrentThread();
        began = Stopwatch.GetTimestamp();
        foreach (string key in cold)
            if (await writes.GetOrSetAsync<long>(key, static (_, _) => Task.FromResult(7L)) != 7)
                throw new InvalidOperationException("Cold value mismatch");
        elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        Console.WriteLine(FormattableString.Invariant($"cold,1,{Warmup},{elapsed / Warmup:F3},{GC.GetAllocatedBytesForCurrentThread() - before}"));
    }
}
