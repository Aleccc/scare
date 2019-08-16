def test_one(stats):
    """top two months must be less than 75% of annual load"""
    try:
        sum_top_two = stats.Normalized.nlargest(2).sum()
        sum_annual = stats.Normalized.sum()
        result = 0.75 > (sum_top_two/sum_annual)
    except TypeError:
        result = False
    return result


def test_two(stats):
    """no month can have a negative value"""
    number_of_negative_months = (stats.Normalized < 0).sum()
    return 0 >= number_of_negative_months


def test_three(stats):
    """no more than 60% of load profile can be zero"""
    return 0.60 > (stats.Normalized <= 0).mean()
