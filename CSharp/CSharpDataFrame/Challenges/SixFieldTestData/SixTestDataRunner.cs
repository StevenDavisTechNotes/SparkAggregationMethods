namespace CSharpDataFrame.Challenges.SixFieldTestData.SixTestDataRunner;

using Microsoft.Data.Analysis;
using Microsoft.Extensions.Logging;
using CSharpDataFrame.Challenges.SixFieldTestData.SixTestDataTypes;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;
using CSharpDataFrame.SharedBetweenChallenges.RunnerTypes;

public class SixTestDataRunnerBase : RunnerBase
{
    private readonly ILogger<SixTestDataRunnerBase> _logger;

    public SixTestDataRunnerBase(ILogger<SixTestDataRunnerBase> logger)
    {
        _logger = logger;
    }

    public DataFrame FetchSixDataSetAnswer(
        Challenge challenge,
        SixTestDataSetDescription dataSize
        )
    {
        var answerFilePaths = SixTestDataSetDescription.SixDeriveExpectedAnswerDataFilePathsCsv(dataSize);
        var answerFilePathCsv = answerFilePaths[challenge];

        _logger.LogInformation($"Loading answer from {answerFilePathCsv}");

        return DataFrame.LoadCsv(filename: answerFilePathCsv);
    }
}