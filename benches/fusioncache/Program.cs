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
    private static void Scaling(FusionCache cache, string[] keys, int workers, bool same)
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
                    if (cache.TryGetAsync<long>(key).GetAwaiter().GetResult().Value != index + 1)
                        throw new InvalidOperationException("Warm value mismatch");
                gate.SignalAndWait();
                gate.SignalAndWait();
                long before = GC.GetAllocatedBytesForCurrentThread();
                long sum = 0;
                for (int n = 0; n < Operations; n++)
                    sum += cache.TryGetAsync<long>(key).GetAwaiter().GetResult().Value;
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
    private static void SynchronousHit()
    {
        using FusionCache cache = New();
        cache.Set("sync", 1L);
        for (int n = 0; n < Warmup; n++)
            if (cache.TryGet<long>("sync").Value != 1) throw new InvalidOperationException("Sync warmup mismatch");
        long before = GC.GetAllocatedBytesForCurrentThread();
        long began = Stopwatch.GetTimestamp();
        long sum = 0;
        for (int n = 0; n < Operations; n++) sum += cache.TryGet<long>("sync").Value;
        double elapsed = Stopwatch.GetElapsedTime(began).TotalNanoseconds;
        if (sum != Operations) throw new InvalidOperationException("Sync checksum mismatch");
        Console.WriteLine(FormattableString.Invariant($"sync,1,{Operations},{elapsed / Operations:F3},{GC.GetAllocatedBytesForCurrentThread() - before}"));
    }
    public static async Task Main()
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
            foreach (bool same in new[] { true, false }) Scaling(cache, keys, workers, same);
        SynchronousHit();
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
