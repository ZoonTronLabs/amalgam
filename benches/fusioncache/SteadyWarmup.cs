using System.Diagnostics;
using System.Text.Json;

// Match benches/scaling/warmup.rs. This is fixture policy, not library code.
internal sealed class SteadyWarmup(string label)
{
    internal const int Batch = 20_000;
    private readonly long started = Stopwatch.GetTimestamp();
    private readonly List<double> windows = new(150);
    private double windowNanoseconds;
    private long windowOperations;
    private long operations;

    internal bool Record(TimeSpan duration, int count)
    {
        operations += count;
        windowOperations += count;
        windowNanoseconds += duration.TotalNanoseconds;
        if (windowNanoseconds >= 100_000_000)
        {
            windows.Add(windowNanoseconds / windowOperations);
            windowNanoseconds = 0;
            windowOperations = 0;
        }
        double seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
        bool stable = windows.Count >= 5
            && windows.TakeLast(5).Max() / windows.TakeLast(5).Min() <= 1.10;
        if (seconds >= 3 && stable || seconds >= 15)
        {
            Console.Error.WriteLine("warmup " + JsonSerializer.Serialize(new
            {
                label, seconds, operations, stable, windows_ns = windows,
            }));
            return true;
        }
        return false;
    }
}
