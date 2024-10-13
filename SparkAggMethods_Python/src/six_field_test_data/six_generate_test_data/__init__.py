from src.six_field_test_data.six_generate_test_data.six_test_data_for_dask import (  # noqa: F401
    ChallengeMethodPythonDaskRegistration, DataSetDask, DataSetDaskWithAnswer, DataSetDataDask,
    IChallengeMethodPythonDask, TChallengeAnswerPythonDask, populate_data_set_dask,
)
from src.six_field_test_data.six_generate_test_data.six_test_data_for_pyspark import (  # noqa: F401
    ISixFieldChallengeMethodPythonPyspark, SixFieldChallengeMethodPythonPysparkRegistration, SixFieldDataSetDataPyspark,
    SixFieldDataSetPyspark, SixFieldDataSetPysparkWithAnswer, TSixFieldChallengePendingAnswerPythonPyspark,
    populate_data_set_pyspark,
)
from src.six_field_test_data.six_generate_test_data.six_test_data_for_python_only import (  # noqa: F401
    ChallengeMethodPythonOnlyRegistration, DataSetDataPythonOnly, DataSetPythonOnly, DataSetPythonOnlyWithAnswer,
    IChallengeMethodPythonOnly, TChallengePythonOnlyAnswer, populate_data_set_python_only,
)
