using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.CommandLine;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;
using System.Text.RegularExpressions;
using CSharpDataFrame.Challenges.Vanilla;

string? DEBUG_ARGUMENTS = """
    --challenges vanilla
    --random-seed 1234
    --shuffle false
    --sizes 3_3_1
    --strategies vanilla_csharp_st_dataframe_naive
    --num-executors 0
""";
using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
ILogger logger = factory.CreateLogger("Program");
logger.LogInformation("Running...");
var serviceProvider =
    new ServiceCollection()
    .AddLogging(options =>
    {
        options.ClearProviders();
        options.AddConsole();
    })
    .AddSingleton<VanillaRunnerCSharpDF>()
    .BuildServiceProvider();

void ActOnArguments()
{
    var argumentsAsString = string.Join(", ", args);
    logger.LogInformation("Arguments: {argumentsAsString}", argumentsAsString);

    var rootCommand = new RootCommand("Performance testing");

    var challengesOption = new Option<string[]>(
        "--challenges",
        getDefaultValue: () => ["vanilla"],
        "Data sizes to test");

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
        "Data sizes to test");

    var strategiesOption = new Option<string[]>(
        "--strategies",
        "Strategy names to test");

    var defaultParallelismOption = new Option<int?>(
        "--parallelism",
        "Shard the data into this many partitions");

    var numExecutorsOption = new Option<int?>(
        "--num-executors",
        "Number of executors to use");

    rootCommand.AddOption(challengesOption);
    rootCommand.AddOption(numRunsOption);
    rootCommand.AddOption(randomSeedOption);
    rootCommand.AddOption(shuffleOption);
    rootCommand.AddOption(sizesOption);
    rootCommand.AddOption(strategiesOption);
    rootCommand.AddOption(defaultParallelismOption);
    rootCommand.AddOption(numExecutorsOption);

    rootCommand.SetHandler(
        (challenges, numRuns, randomSeed, shuffle, sizes, strategies, defaultParallelism, numExecutors)
        =>
        {
            var generic_args = new GenericArgumentsBase(
                NumRuns: numRuns,
                RandomSeed: randomSeed,
                Shuffle: shuffle,
                Sizes: sizes,
                StrategyNames: strategies,
                DefaultParallelism: defaultParallelism ?? 2 * PerfTestCommonConstants.LOCAL_NUM_EXECUTORS,
                NumExecutors: numExecutors ?? PerfTestCommonConstants.LOCAL_NUM_EXECUTORS
            );
            foreach (var challenge in challenges)
            {
                switch (challenge)
                {
                    case "bi_level":
                        throw new NotImplementedException();
                    // serviceProvider
                    // .GetRequiredService<BiLevelRunnerCSharpDF>()
                    // .Main(generic_args);
                    // break;
                    case "conditional":
                        throw new NotImplementedException();
                    // serviceProvider
                    // .GetRequiredService<ConditionalRunnerCSharpDF>()
                    // .Main(generic_args);
                    // break;
                    case "dedupe":
                        throw new NotImplementedException();
                    // serviceProvider
                    // .GetRequiredService<DedupeRunnerCSharpDF>()
                    // .Main(generic_args);
                    // break;
                    case "section":
                        throw new NotImplementedException();
                    // serviceProvider
                    // .GetRequiredService<SectionRunnerCSharpDF>()
                    // .Main(generic_args);
                    // break;
                    case "vanilla":
                        serviceProvider
                        .GetRequiredService<VanillaRunnerCSharpDF>()
                        .Main(generic_args);
                        break;
                    default:
                        throw new ArgumentException($"Unknown challenge {challenge}");
                }
            }
            serviceProvider.GetRequiredService<VanillaRunnerCSharpDF>().Main(generic_args);
        },
        challengesOption, numRunsOption, randomSeedOption, shuffleOption, sizesOption, strategiesOption, defaultParallelismOption, numExecutorsOption
    );
    string[] arguments_to_use;
    if (DEBUG_ARGUMENTS != null) {
        var draft = DEBUG_ARGUMENTS.Trim();
        draft = Regex.Replace(draft, @"\s+", " ");
        arguments_to_use = draft.Split(' ');
    } else {
        arguments_to_use = args;
    }
    rootCommand.Invoke(arguments_to_use);
}
ActOnArguments();

logger.LogInformation("Done");
// Return a success code.
return 0;