from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfaceScalaSpark,
    SolutionLanguage,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldChallengeMethodScalaSparkRegistration,
    TSixFieldChallengePendingAnswerPythonPyspark,
)


def ScalaSparkMethodPlaceholder() -> TSixFieldChallengePendingAnswerPythonPyspark:
    raise NotImplementedError("ScalaSparkMethodPlaceholder")


VANILLA_STRATEGY_REGISTRY_SCALA = [
    SixFieldChallengeMethodScalaSparkRegistration(
        strategy_name_2018='vanilla_pyspark_sql',
        strategy_name='vanilla_sc_spark_sql',
        language=SolutionLanguage.SCALA,
        engine=CalcEngine.SCALA_SPARK,
        interface=SolutionInterfaceScalaSpark.SPARK_SQL,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=ScalaSparkMethodPlaceholder,
    ),
    SixFieldChallengeMethodScalaSparkRegistration(
        strategy_name_2018='vanilla_fluent',
        strategy_name='vanilla_sc_spark_fluent',
        language=SolutionLanguage.SCALA,
        engine=CalcEngine.SCALA_SPARK,
        interface=SolutionInterfaceScalaSpark.SPARK_SQL,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=ScalaSparkMethodPlaceholder,
    ),
    SixFieldChallengeMethodScalaSparkRegistration(
        strategy_name_2018='vanilla_udaf',
        strategy_name='vanilla_sc_spark_udaf',
        language=SolutionLanguage.SCALA,
        engine=CalcEngine.SCALA_SPARK,
        interface=SolutionInterfaceScalaSpark.SPARK_SQL,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=ScalaSparkMethodPlaceholder,
    ),
    SixFieldChallengeMethodScalaSparkRegistration(
        strategy_name_2018='vanilla_rdd_grpmap',
        strategy_name='vanilla_sc_spark_rdd_grpmap',
        language=SolutionLanguage.SCALA,
        engine=CalcEngine.SCALA_SPARK,
        interface=SolutionInterfaceScalaSpark.SPARK_RDD,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=ScalaSparkMethodPlaceholder,
    ),
    SixFieldChallengeMethodScalaSparkRegistration(
        strategy_name_2018='vanilla_rdd_reduce',
        strategy_name='vanilla_sc_spark_rdd_reduce',
        language=SolutionLanguage.SCALA,
        engine=CalcEngine.SCALA_SPARK,
        interface=SolutionInterfaceScalaSpark.SPARK_RDD,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=ScalaSparkMethodPlaceholder,
    ),
    SixFieldChallengeMethodScalaSparkRegistration(
        strategy_name_2018='vanilla_rdd_mappart',
        strategy_name='vanilla_sc_spark_rdd_mappart',
        language=SolutionLanguage.SCALA,
        engine=CalcEngine.SCALA_SPARK,
        interface=SolutionInterfaceScalaSpark.SPARK_RDD,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=ScalaSparkMethodPlaceholder,
    ),
]
