import math

from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    DataPointNT, SubTotalDC, TotalDC)


def zero_subtotal(
) -> SubTotalDC:
    return SubTotalDC(
        running_sum_of_C=0,
        running_count=0,
        running_max_of_D=-math.inf,
        running_sum_of_E_squared=0,
        running_sum_of_E=0,
    )


def accumulate_subtotal(
        prior: SubTotalDC | None,
        element: DataPointNT,
) -> SubTotalDC:
    prior = prior if prior is not None else zero_subtotal()
    updated = SubTotalDC(
        running_sum_of_C=prior.running_sum_of_C + element.C,
        running_count=prior.running_count + 1,
        running_max_of_D=max(prior.running_max_of_D, element.D),
        running_sum_of_E_squared=prior.running_sum_of_E_squared + element.E**2,
        running_sum_of_E=prior.running_sum_of_E + element.E,
    )
    return updated


def combine_subtotals(
        subtotal1: SubTotalDC | None,
        subtotal2: SubTotalDC | None,
) -> SubTotalDC:
    if subtotal1 is None:
        if subtotal2 is None:
            raise ValueError("At least one subtotal must be non-None")
        return subtotal2
    elif subtotal2 is None:
        return subtotal1
    combined = SubTotalDC(
        running_sum_of_C=subtotal1.running_sum_of_C + subtotal2.running_sum_of_C,
        running_count=subtotal1.running_count + subtotal2.running_count,
        running_max_of_D=max(subtotal1.running_max_of_D, subtotal2.running_max_of_D),
        running_sum_of_E_squared=subtotal1.running_sum_of_E_squared + subtotal2.running_sum_of_E_squared,
        running_sum_of_E=subtotal1.running_sum_of_E + subtotal2.running_sum_of_E,
    )
    return combined


def total_from_subtotal(
        subtotal: SubTotalDC,
) -> TotalDC:
    num = subtotal.running_count
    var_of_E2 = (
        subtotal.running_sum_of_E_squared / subtotal.running_count
        - (subtotal.running_sum_of_E / subtotal.running_count)**2)
    result = TotalDC(
        mean_of_C=subtotal.running_sum_of_C / num,
        max_of_D=subtotal.running_max_of_D,
        var_of_E=var_of_E2,
        var_of_E2=var_of_E2,
    )
    return result
