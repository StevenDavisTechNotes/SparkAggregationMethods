namespace CSharpDataFrame.Challenges.Vanilla;

using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;
using CSharpDataFrame.SharedBetweenChallenges.RunnerTypes;
using System;
using System.IO;
public record VanillaRunResult(
    int NumSourceRows,
    double ElapsedTime,
    int NumOutputRows,
    string? FinishedAt
) : RunResultBase(NumSourceRows, ElapsedTime, NumOutputRows, FinishedAt);

public record VanillaPersistedRunResult(
    int NumSourceRows,
    double ElapsedTime,
    int NumOutputRows,
    string? FinishedAt,
    SolutionLanguage Language,
    CalcEngine Engine,
    CSharpSolutionInterface SolutionInterface,
    string StrategyName
) : PersistedRunResultBase<CSharpSolutionInterface>(
    NumSourceRows,
    ElapsedTime,
    NumOutputRows,
    FinishedAt,
    Language,
    Engine,
    SolutionInterface,
    StrategyName
)
{
    public static int RegressorFromRunResult(VanillaPersistedRunResult result)
    {
        return result.NumSourceRows;
    }
};


public class VanillaRunResultFileWriter
: RunResultFileWriterBase<CSharpSolutionInterface>, IDisposable
{
    public VanillaRunResultFileWriter(CalcEngine engine, string relLogFilePath)
        : base(
            Path.GetFullPath(relLogFilePath),
            SolutionLanguage.CSHARP,
            engine,
            typeof(VanillaPersistedRunResult)
            )
    {
    }

    public override void WriteRunResult(
        ChallengeMethodBase challengeMethod,
        RunResultBase runResult
    )
    {
        if (runResult is not VanillaRunResult vanillaRunResult)
            throw new ArgumentException("Invalid run result type", nameof(runResult));

        if (Engine != challengeMethod.Engine)
            throw new InvalidOperationException("Engine mismatch");

        var persistedRunResult = new VanillaPersistedRunResult(
            vanillaRunResult.NumSourceRows,
            vanillaRunResult.ElapsedTime,
            vanillaRunResult.NumOutputRows,
            DateTime.Now.ToString("o"),
            this.Language,
            challengeMethod.Engine,
            challengeMethod.SolutionInterface,
            challengeMethod.StrategyName
        );

        PersistRunResult(persistedRunResult);
    }
}