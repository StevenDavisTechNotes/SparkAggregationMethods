from typing import cast

import numpy as np

from spark_agg_methods_common_python.utils.progressive_statistics.progressive_variance import ProgressiveVariance


class Test_ProgressiveVariance:
    def test_default_option_with_unbiases_variance_and_along_no_axis(self):
        inc_mean_var = ProgressiveVariance()
        assert inc_mean_var.update_with_population(np.array([2, 8, 7, 4, 5])) == (5.2, 5.7)
        assert inc_mean_var.update_with_population(np.array([-3, 5, 2, 6])) == (4.0, 11.0)

    def test_biased_variance(self):
        inc_mean_var = ProgressiveVariance(ddof=0)
        assert inc_mean_var.update_with_population(np.array([2, 8, 7, 4, 5])) == (5.2, 4.5600000000000005)
        assert inc_mean_var.update_with_population(np.array([-3, 5, 2, 6])) == (4.0, 9.777777777777779)

    def test_along_axis_0(self):
        inc_mean_var = ProgressiveVariance(axis=0)
        result = cast(tuple[np.ndarray, np.ndarray],
                      inc_mean_var.update_with_population(np.array([
                          [2, 8, 7],
                          [-1, 10, 3],
                          [-7, 18, 5]
                      ])))
        assert result[0].tolist() == [-2., 12., 5.]
        assert result[1].tolist() == [21., 28., 4.]
        result = cast(tuple[np.ndarray, np.ndarray],
                      inc_mean_var.update_with_population(np.array([
                          [8, 2, 5],
                          [-16, 4, 7]
                      ])))
        assert result[0].tolist() == [-2.8, 8.4, 5.4]
        assert result[1].tolist() == [83.7, 38.8, 2.8]

    def test_different_batch_sizes_should_produce_similar_answers(self):
        # Arrange
        batch_size = 1000
        num_batches = 100
        np.random.seed(0)
        data = np.random.normal(loc=0, scale=1, size=(batch_size, num_batches))
        pv = ProgressiveVariance(ddof=0)
        for batch in data:
            pv.update_with_population(batch)
        mean1 = pv.mean
        var1 = pv.variance
        pv.reset()
        for batch in data.T:
            pv.update_with_population(batch)
        mean2 = pv.mean
        var2 = pv.variance
        # Act
        # Assert
        assert np.allclose(mean1, mean2)  # cSpell: ignore allclose
        assert np.allclose(var1, var2)  # cSpell: ignore allclose
