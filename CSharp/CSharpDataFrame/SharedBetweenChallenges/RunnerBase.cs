using System.Diagnostics;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;

public abstract class RunResultFileWriterBase<TSolutionInterface> : IDisposable
{
    private StreamWriter _file ;
    public SolutionLanguage Language { get; }
    public CalcEngine Engine { get; }
    protected readonly Type _persistedRowType;

    protected RunResultFileWriterBase(
        string logFilePath,
        SolutionLanguage language,
        CalcEngine engine,
        Type persistedRowType)
    {
        _file = new StreamWriter(logFilePath, true);
        Language = language;
        Engine = engine;
        _persistedRowType = persistedRowType;
        WriteHeader();
    }

    private void WriteHeader()
    {
        var header = " " + string.Join(",",
            _persistedRowType.GetProperties()
            .Select(p => p.Name)
            .OrderBy(n => n)) + ",";

        if (Debugger.IsAttached)
            Console.WriteLine($"Skipping writing header {header}");
        else
        {
            Console.WriteLine($"Writing header {header}");
            _file.WriteLine(header);
        }
    }

    public abstract void WriteRunResult(
        ChallengeMethodBase challengeMethod,
        RunResultBase runResult);

    protected void PersistRunResult(
        PersistedRunResultBase<TSolutionInterface> persistedResultRow)
    {
        var properties = persistedResultRow.GetType().GetProperties()
            .OrderBy(p => p.Name);

        var line = string.Join(",",
            properties.Select(p =>
                p.GetValue(persistedResultRow)?.ToString() ?? "")) + ",";

        if (Debugger.IsAttached)
        {
            Console.WriteLine($"Skipping writing line {line}");
        }
        else
        {
            Console.WriteLine($"Writing result {line}");
            _file.WriteLine(line);
            _file.Flush();
        }
    }

    public void Dispose()
    {
        if (_file != null)
        {
            _file.Flush();
            _file.Dispose();
            _file = null!;
        }
        GC.SuppressFinalize(this);
    }
}


public class RunnerBase
{
    public static (string strategyName, string sizeCode)[] AssembleItinerary(RunnerArgumentsBase args)
    {
        var itinerary = (
            from strategyName in args.StrategyNames
            from sizeCode in args.Sizes
            from run in Enumerable.Range(0, args.NumRuns)
            select (strategyName, sizeCode)).ToArray();
        var random = args.RandomSeed.HasValue ? new Random(args.RandomSeed.Value) : new Random();
        if (args.Shuffle)
        {
            random.Shuffle(itinerary);
        }

        return itinerary;

    }
}