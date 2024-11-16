# from https://gist.github.com/DevSlem/555e7caf4b843741682fbff64ae1cf15

from typing import Optional, Tuple, Union

import numpy as np


class ProgressiveVariance:
    """
    ## Summary

    Incremental mean and variance calculation from batch.
    See details in https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm.

    Args:
        ddof (int, optional): delta degrees of freedom (DDOF)
            - especially 0 means biased variance, 1 means unbiased variance.
            Defaults to 1.
        axis (int | None, optional): axis along which mean and variance are computed.
            The default is to compute the values of the flattened array.

    ## Example

    default option with unbiased variance and along no axis::

        inc_mean_var = IncrementalMeanVarianceFromBatch()
        >>> inc_mean_var.update(np.array([2, 8, 7, 4, 5]))
        (5.2, 5.7)
        >>> inc_mean_var.update(np.array([-3, 5, 2, 6]))
        (4.0, 11.0)

    biased variance::

        inc_mean_var = IncrementalMeanVarianceFromBatch(ddof=0)
        >>> inc_mean_var.update(np.array([2, 8, 7, 4, 5]))
        (5.2, 4.5600000000000005)
        >>> inc_mean_var.update(np.array([-3, 5, 2, 6]))
        (4.0, 9.777777777777779)

    along axis 0 (it's useful when you want to compute online the mean and variance of each feature
    with many divided batches)::

        inc_mean_var = IncrementalMeanVarianceFromBatch(axis=0)
        >>> inc_mean_var.update(np.array([[2, 8, 7],
                                          [-1, 10, 3],
                                          [-7, 18, 5]]))
        (array([-2., 12.,  5.]), array([21., 28.,  4.]))
        >>> inc_mean_var.update(np.array([[8, 2, 5],
                                          [-16, 4, 7]]))
        (array([-2.8,  8.4,  5.4]), array([83.7, 38.8,  2.8]))
    """

    def __init__(self, ddof: int = 1, axis: Optional[int] = None) -> None:
        self._ddof = ddof
        self._axis = axis
        self.reset()

    @property
    def mean(self) -> Union[float, np.ndarray]:
        return self._mean

    @property
    def variance(self) -> Union[float, np.ndarray]:
        return self._var

    @property
    def batch_size(self) -> int:
        return self._n

    def reset(self):
        """Reset the mean and variance to zero."""
        self._mean = 0.0
        self._var = 0.0
        self._n = 0

    def update(
            self,
            batch: np.ndarray,
    ) -> Tuple[Union[float, np.ndarray], Union[float, np.ndarray]]:
        """
        Update the mean and variance from a batch of data.

        Returns:
            mean (float | np.ndarray): updated mean
            variance (float | np.ndarray): updated variance
        """
        batch_size = batch.size if self._axis is None else batch.shape[self._axis]
        if batch_size == 0:
            return self._mean, self._var
        batch_mean = batch.mean(axis=self._axis)
        batch_var = batch.var(ddof=self._ddof, axis=self._axis)
        return self._update_from_batch_mean_var(
            batch_mean=batch_mean,
            batch_var=batch_var,
            batch_size=batch_size,
        )

    def merge_subtotals(
            self,
            other: 'ProgressiveVariance',
    ) -> Tuple[Union[float, np.ndarray], Union[float, np.ndarray]]:
        if other.batch_size == 0:
            return self._mean, self._var
        if self._n == 0:
            self._mean = other.mean
            self._var = other.variance
            self._n = other.batch_size
            return self._mean, self._var
        return self._update_from_batch_mean_var(
            batch_mean=other.mean,
            batch_var=other.variance,
            batch_size=other.batch_size,
        )

    def _update_from_batch_mean_var(
            self,
            *,
            batch_mean: Union[float, np.ndarray],
            batch_var: Union[float, np.ndarray],
            batch_size: int,
    ) -> Tuple[Union[float, np.ndarray], Union[float, np.ndarray]]:
        # n: batch size
        # M: sum of squares
        # a: old batch
        # b: new batch
        # d: DDOF
        d = self._ddof
        n_a = self._n
        n_b = batch_size

        if n_a == 0:
            self._mean = batch_mean
            self._var = batch_var
            self._n = n_b
            return self._mean, self._var
        elif n_b == 0:
            return self._mean, self._var

        n = n_a + n_b

        # update mean
        delta = batch_mean - self._mean
        self._mean = self._mean + delta * n_b / n

        # update variance
        M_a = self._var * (n_a - d)
        M_b = batch_var * (n_b - d)
        M = M_a + M_b + delta**2 * n_a * n_b / n
        self._var = M / (n - d)

        # update batch size
        self._n = n

        return self._mean, self._var
