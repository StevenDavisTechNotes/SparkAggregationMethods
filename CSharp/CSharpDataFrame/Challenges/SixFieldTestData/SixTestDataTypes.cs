namespace CSharpDataFrame.Challenges.SixFieldTestData.SixTestDataTypes;

using System.Collections.Generic;
using System.IO;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;
using Microsoft.Data.Analysis;

public static class SixFieldTestDataConstants
{
    public static readonly Challenge[] SIX_TEST_CHALLENGES = new Challenge[]
    {
        Challenge.BI_LEVEL,
        Challenge.CONDITIONAL,
        Challenge.VANILLA
    };
    public const int TARGET_PARQUET_BATCH_SIZE = 65536;
}

public record DataPoint
{
    public int Id { get; init; }
    public int Grp { get; init; }
    public int SubGrp { get; init; }
    public float A { get; init; }
    public float B { get; init; }
    public float C { get; init; }
    public float D { get; init; }
    public float E { get; init; }
    public float F { get; init; }
}

public record SubTotal
{
    public int RunningCount { get; init; }
    public double RunningMaxOfD { get; init; }
    public double RunningSumOfC { get; init; }
    public double RunningSumOfESquared { get; init; }
    public double RunningSumOfE { get; init; }
}


public record TotalDC(
double MeanOfC,
double MaxOfD,
double VarOfE,
double VarOfE2
);


public abstract class SixTestDataSetDescription : DataSetDescriptionBase
{
    public int NumGrp1 { get; init; }
    public int NumGrp2 { get; init; }
    public int PointsPerIndex { get; init; }
    public int RelativeCardinalityBetweenGroups => NumGrp2 / NumGrp1;

    public SixTestDataSetDescription(
    bool debuggingOnly, int numGrp1, int numGrp2, int pointsPerIndex, string sizeCode
    ) : base(
    debuggingOnly: debuggingOnly,
    numSourceRows: numGrp1 * numGrp2 * pointsPerIndex,
    sizeCode: sizeCode
    )
    { }


    public static Dictionary<Challenge, string> SixDeriveExpectedAnswerDataFilePathsCsv(
        SixTestDataSetDescription dataDescription,
        bool tempFile = false)
    {
        int numGrp1 = dataDescription.NumGrp1;
        int numGrp2 = dataDescription.NumGrp2;
        int repetition = dataDescription.PointsPerIndex;
        string tempPostfix = tempFile ? "_temp" : "";

        var answerFileNames = new Dictionary<Challenge, string>();
        foreach (var challenge in SixFieldTestDataConstants.SIX_TEST_CHALLENGES)
        {
            string filePath = Path.Combine(
                PerfTestCommonConstants.LOCAL_TEST_DATA_FILE_LOCATION,
                "SixField_Test_Data",
                $"{challenge}_answer_data_{numGrp1}_{numGrp2}_{repetition}{tempPostfix}.csv");
            answerFileNames[challenge] = filePath;
        }

        return answerFileNames;
    }


    public static SixTestSourceDataFilePaths SixDeriveSourceTestDataFilePath(
        SixTestDataSetDescription dataDescription,
        bool tempFile = false)
    {
        int numGrp1 = dataDescription.NumGrp1;
        int numGrp2 = dataDescription.NumGrp2;
        int repetition = dataDescription.PointsPerIndex;
        string tempPostfix = tempFile ? "_temp" : "";

        string sourceDirectoryPath = Path.Combine(
            PerfTestCommonConstants.LOCAL_TEST_DATA_FILE_LOCATION,
            "SixField_Test_Data"
        );

        string stem = Path.Combine(
            sourceDirectoryPath,
            $"six_field_source_data_{numGrp1}_{numGrp2}_{repetition}{tempPostfix}"
        );

        return new SixTestSourceDataFilePaths(
            sourceDirectoryPath,
            $"{stem}_spark",
            $"{stem}_modern.parquet",
            $"{stem}.csv"
        );
    }

}


public class SixTestDataSetAnswers
{
    public DataFrame BiLevelAnswer { get; }
    public DataFrame ConditionalAnswer { get; }
    public DataFrame VanillaAnswer { get; }

    public SixTestDataSetAnswers(DataFrame biLevelAnswer, DataFrame conditionalAnswer, DataFrame vanillaAnswer)
    {
        BiLevelAnswer = biLevelAnswer;
        ConditionalAnswer = conditionalAnswer;
        VanillaAnswer = vanillaAnswer;
    }

    public DataFrame AnswerForChallenge(Challenge challenge)
    {
        return challenge switch
        {
            Challenge.VANILLA => VanillaAnswer,
            Challenge.BI_LEVEL => BiLevelAnswer,
            Challenge.CONDITIONAL => ConditionalAnswer,
            _ => throw new KeyNotFoundException($"Unknown challenge {challenge}")
        };
    }
}

public record SixTestExecutionParameters(
    int defaultParallelism, int numExecutors
) : ExecutionParametersBase(defaultParallelism, numExecutors);


public record SixTestDataSetWAnswers(
    int NumSourceRows,
    int SrcNumPartitions,
    int TgtNumPartitions1Level,
    int TgtNumPartitions2Level,
    DataFrame DfSrc,
    DataFrame VanillaAnswer,
    DataFrame BilevelAnswer,
    DataFrame ConditionalAnswer
);


public class SixTestDataChallengeMethod : ChallengeMethodBase
{
    public double NumericalTolerance { get; init; }

    public SixTestDataChallengeMethod(
        string strategyName,
        SolutionLanguage language,
        CalcEngine engine,
        CSharpSolutionInterface solutionInterface,
        bool requiresGpu,
        double NumericalTolerance
    ) : base(strategyName, language, engine, solutionInterface, requiresGpu)
    {
        this.NumericalTolerance = NumericalTolerance;
    }
}

public class SixTestSourceDataFilePaths
{
    public string SourceDirectoryPath { get; }
    public string SourceFilePathParquetSmallV1Files { get; }
    public string SourceFilePathParquetSingleFile { get; }
    public string SourceFilePathCsv { get; }

    public SixTestSourceDataFilePaths(
        string sourceDirectoryPath,
        string sourceFilePathParquetSmallV1Files,
        string sourceFilePathParquetSingleFile,
        string sourceFilePathCsv)
    {
        SourceDirectoryPath = sourceDirectoryPath;
        SourceFilePathParquetSmallV1Files = sourceFilePathParquetSmallV1Files;
        SourceFilePathParquetSingleFile = sourceFilePathParquetSingleFile;
        SourceFilePathCsv = sourceFilePathCsv;
    }

    public string[] FilePaths => [
        SourceFilePathParquetSmallV1Files,
        SourceFilePathParquetSingleFile,
        SourceFilePathCsv
    ];
}

