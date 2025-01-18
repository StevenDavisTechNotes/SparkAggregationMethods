namespace CSharpDataFrame.SharedBetweenChallenges.CommonTypes;

using System.ComponentModel;

static class PerfTestCommonConstants
{
    public const string ELAPSED_TIME_COLUMN_NAME = "elapsed_time";
    public const string LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting";
    public const int LOCAL_NUM_EXECUTORS = 5;
}

public enum SolutionLanguage
{
    [Description("csharp")]
    CSHARP
}

public enum CalcEngine
{
    [Description("parquet")]
    PARQUET_DOT_NET,
    [Description("df")]
    DATA_FRAME,
    [Description("native")]
    NATIVE
    // [Description("c_spark")]
    // SPARK_DOT_NET
}

public enum Challenge
{
    [Description("vanilla")]
    VANILLA,
    [Description("bilevel")]
    BI_LEVEL,
    [Description("conditional")]
    CONDITIONAL,
    [Description("sectional")]
    SECTIONAL,
    [Description("deduplication")]
    DEDUPLICATION
}

public enum CSharpSolutionInterface
{
    [Description("invalid")]
    INVALID,
    [Description("parquet_row")]
    PARQUET_ROW_BASED,
    [Description("parquet_column")]
    PARQUET_COLUMN_BASED,
    [Description("msft_data_frame")]
    MSFT_DATA_FRAME,
    [Description("none")]
    NONE
}

public static class NumericalToleranceExpectations
{
    public const double NOT_APPLICABLE = -1.0;
    public const double NUMPY = 1e-12;
    public const double NUMBA = 1e-10;
    public const double SIMPLE_SUM = 1e-11;
}


public record ChallengeMethodBase
{
    public string StrategyName { get; }
    public SolutionLanguage Language { get; }
    public CalcEngine Engine { get; }
    public CSharpSolutionInterface SolutionInterface { get; }
    public bool requires_gpu { get; }

    public ChallengeMethodBase(
        string strategyName,
        SolutionLanguage language,
        CalcEngine engine,
        CSharpSolutionInterface solutionInterface,
        bool requiresGpu
    )
    {
        StrategyName = strategyName ?? throw new ArgumentNullException(nameof(strategyName));
        this.Language = language;
        this.Engine = engine;
        this.SolutionInterface = solutionInterface;
        requires_gpu = requiresGpu;
    }
}

public record ChallengeMethodCSharp : ChallengeMethodBase
{
    public ChallengeMethodCSharp(
        string strategyName,
        CalcEngine engine,
        CSharpSolutionInterface solutionInterface,
        bool requiresGpu
    ) : base(
        strategyName: strategyName, 
        language: SolutionLanguage.CSHARP, 
        engine: engine, 
        solutionInterface: solutionInterface, 
        requiresGpu: requiresGpu
        )
    {
    }
}


public abstract class DataSetDescriptionBase
{
    public bool DebuggingOnly { get; }
    public int NumSourceRows { get; }
    public string SizeCode { get; }
    abstract public string RegressorFieldName { get; }
    abstract public int RegressorValue { get; }

    protected DataSetDescriptionBase(
        bool debuggingOnly,
        int numSourceRows,
        string sizeCode
    )
    {
        DebuggingOnly = debuggingOnly;
        NumSourceRows = numSourceRows;
        SizeCode = sizeCode;
    }
}


public record ExecutionParametersBase(
    int DefaultParallelism,
    int NumExecutors
);

public record DedupeExecutionParameters(
    int DefaultParallelism,
    int NumExecutors,
    bool InCloudMode,
    bool CanAssumeNoDupesPerPartition
) : ExecutionParametersBase(DefaultParallelism, NumExecutors);
public record SectionExecutionParameters(
    int DefaultParallelism,
    int NumExecutors
) : ExecutionParametersBase(DefaultParallelism, NumExecutors);
public record SixTestExecutionParameters(
    int DefaultParallelism,
    int NumExecutors
) : ExecutionParametersBase(DefaultParallelism, NumExecutors);


public record RunResultBase(
    int NumSourceRows,
    double ElapsedTime,
    int NumOutputRows,
    string? FinishedAt
);

public record PersistedRunResultBase<TSolutionInterface>(
    int NumSourceRows,
    double ElapsedTime,
    int NumOutputRows,
    string? FinishedAt,
    SolutionLanguage Language,
    CalcEngine Engine,
    TSolutionInterface SolutionInterface,
    string StrategyName
) : RunResultBase(NumSourceRows, ElapsedTime, NumOutputRows, FinishedAt);


public record RunnerArgumentsBase(
    int NumRuns,
    int? RandomSeed,
    bool Shuffle,
    string[] Sizes,
    string[] StrategyNames
);

public record GenericArgumentsBase(
    int NumRuns,
    int? RandomSeed,
    bool Shuffle,
    string[] Sizes,
    string[] StrategyNames,
    int? DefaultParallelism,
    int? NumExecutors
);
