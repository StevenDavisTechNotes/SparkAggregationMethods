import statistics
from dataclasses import dataclass

import numpy
from scipy.stats import chi2 as scipy_stats_chi2  # type: ignore
from scipy.stats import t as scipy_stats_t  # type: ignore


@dataclass(frozen=True)
class RegressionRange:
    b_predicted: float
    b_low: float
    b_high: float


@dataclass(frozen=True)
class CondResult:
    name: str
    interface: str
    b0: RegressionRange
    b1: RegressionRange
    s2: RegressionRange


LinearRegressionResult = tuple[
    tuple[float, tuple[float, float]],
    tuple[float, tuple[float, float]],
    tuple[float, tuple[float, float]],
]

# based on https://gist.github.com/riccardoscalco/5356167


def linear_regression(
        x_in: list[float],
        y_in: list[float],
        prob: float,
) -> LinearRegressionResult | None:
    """
    Return the linear regression parameters and their <prob> confidence intervals.
    ex:
    >>> linear_regression([.1,.2,.3],[10,11,11.5],0.95)
    """
    x = numpy.array(x_in)
    y = numpy.array(y_in)
    n = len(x)
    if n < 3:
        return None

    xy = x * y
    xx = x * x

    xx_mean = xx.mean()
    xy_mean = xy.mean()
    x_mean = x.mean()
    y_mean = y.mean()

    # estimates

    b1 = (xy_mean - x_mean * y_mean) / (xx_mean - x_mean**2)
    b0 = y_mean - b1 * x_mean
    s2 = statistics.mean([(y[i] - b0 - b1 * x[i])**2 for i in range(n)])

    # confidence intervals

    alpha = 1 - prob
    c1 = scipy_stats_chi2.ppf(  # type: ignore
        alpha / 2., n - 2)
    c2 = scipy_stats_chi2.ppf(  # type: ignore
        1 - alpha / 2., n - 2)
    s2_low = n * s2 / c2
    s2_high = n * s2 / c1

    c = -1 * scipy_stats_t.ppf(  # type: ignore
        alpha / 2., n - 2)
    bb1 = c * (s2 / ((n - 2) * (xx_mean - x_mean**2)))**.5

    bb0 = c * ((s2 / (n - 2)) * (1 + x_mean**2 / (xx_mean - x_mean**2)))**.5
    return (
        (b0, (b0 - bb0, b0 + bb0)),
        (b1, (b1 - bb1, b1 + bb1)),
        (s2, (s2_low, s2_high)))
