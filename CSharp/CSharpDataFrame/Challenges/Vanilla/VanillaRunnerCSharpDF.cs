using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Challenges.Vanilla.VanillaRunnerCSharpDF;

using Microsoft.Data.Analysis;
using System;
using System.CommandLine;
using System.IO;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;
using CSharpDataFrame.Challenges.SixFieldTestData.SixTestDataRunner;

public record VanillaRunnerArguments(
    int NumRuns,
    int? RandomSeed,
    bool Shuffle,
    List<string> Sizes,
    List<string> StrategyNames,
    SixTestExecutionParameters ExecParams
) : RunnerArgumentsBase(NumRuns, RandomSeed, Shuffle, Sizes, StrategyNames);

public class VanillaRunnerCSharpDF
{
    private readonly ILogger<VanillaRunnerCSharpDF> _logger;

    public VanillaRunnerCSharpDF(ILogger<VanillaRunnerCSharpDF> logger)
    {
        _logger = logger;
    }

    public void Main(IList<string> commandLineArguments)
    {
        UpdateChallengeRegistration();
        ExecuteArguments(commandLineArguments);
        _logger.LogInformation($"Done with {this.GetType().Name}");
    }
    public void ExecuteArguments(IList<string> commandLineArguments)
    {
        var rootCommand = new RootCommand("Performance testing");

        var numRunsOption = new Option<int>(
            "--num-runs",
            getDefaultValue: () => 1,
            "Number of test runs to execute");

        var randomSeedOption = new Option<int?>(
            "--random-seed",
            "Random seed for shuffling test cases");

        var shuffleOption = new Option<bool>(
            "--shuffle",
            getDefaultValue: () => false,
            "Whether to shuffle the test cases");

        var sizesOption = new Option<string[]>(
            "--sizes",
            getDefaultValue: () => new[] { "xs" },
            "Data sizes to test");

        var strategiesOption = new Option<string[]>(
            "--strategies",
            getDefaultValue: () => new[] { "baseline" },
            "Strategy names to test");

        rootCommand.AddOption(numRunsOption);
        rootCommand.AddOption(randomSeedOption);
        rootCommand.AddOption(shuffleOption);
        rootCommand.AddOption(sizesOption);
        rootCommand.AddOption(strategiesOption);

        rootCommand.SetHandler((numRuns, randomSeed, shuffle, sizes, strategies) =>
        {
            var args = new VanillaRunnerArguments(
                NumRuns: numRuns,
                RandomSeed: randomSeed,
                Shuffle: shuffle,
                Sizes: sizes.ToList(),
                StrategyNames: strategies.ToList(),
                ExecParams: new SixTestExecutionParameters(
                    DefaultParallelism: 2 * PerfTestCommonConstants.LOCAL_NUM_EXECUTORS,
                    NumExecutors: PerfTestCommonConstants.LOCAL_NUM_EXECUTORS
                )
            );

            DoTestRuns(args);

        }, numRunsOption, randomSeedOption, shuffleOption, sizesOption, strategiesOption);
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