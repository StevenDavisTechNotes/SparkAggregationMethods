namespace CSharpDataFrame.Challenges.Vanilla;

using System;
using Microsoft.Extensions.Logging;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;

public record VanillaRunnerArguments(
    int NumRuns,
    int? RandomSeed,
    bool Shuffle,
    string[] Sizes,
    string[] StrategyNames,
    SixTestExecutionParameters ExecParams
) : RunnerArgumentsBase(NumRuns, RandomSeed, Shuffle, Sizes, StrategyNames)
{
    public static VanillaRunnerArguments FromCommandLineArguments(GenericArgumentsBase commandLineArguments)
    {
        return new VanillaRunnerArguments(
            NumRuns: commandLineArguments.NumRuns,
            RandomSeed: commandLineArguments.RandomSeed,
            Shuffle: commandLineArguments.Shuffle,
            Sizes: commandLineArguments.Sizes,
            StrategyNames: commandLineArguments.StrategyNames,
            ExecParams: new SixTestExecutionParameters(
                commandLineArguments.DefaultParallelism ?? 1,
                commandLineArguments.NumExecutors ?? 1
            )
        );
    }
};

public class VanillaRunnerCSharpDF
{
    private readonly ILogger<VanillaRunnerCSharpDF> _logger;

    public VanillaRunnerCSharpDF(ILogger<VanillaRunnerCSharpDF> logger)
    {
        _logger = logger;
    }

    public void Main(GenericArgumentsBase commandLineArguments)
    {
        UpdateChallengeRegistration();
        var args = VanillaRunnerArguments.FromCommandLineArguments(commandLineArguments);
        DoTestRuns(args);
        var runnerName = this.GetType().Name;
        _logger.LogInformation("Done with {runnerName}", runnerName);
    }
    public void UpdateChallengeRegistration()
    {
        throw new NotImplementedException();
    }
    public void DoTestRuns(VanillaRunnerArguments args)
    {
        throw new NotImplementedException();
    }
}