import pandas as pd
from datawarehouse.queries import query_premises
from weather.noaa_api import get_data
from scare.scare import scare_result
from timer.timer import timer

YEAR = 2019
MONTH = 8


@timer
def retrieve_reads():
    """Return two years worth of meter reads for only Gas South customers."""
    reads = query_premises.get_reads_for_gas_south_customers(YEAR, MONTH)
    reads = reads.append(query_premises.get_reads_for_gas_south_customers(YEAR, MONTH, year_prior=True))
    # # export as csv
    # reads.to_csv('reads.csv', index=False)
    reads = reads.reset_index(drop=True)
    return reads


@timer
def retrieve_weather_and_clean(update_weather=False):
    weather = get_data(update_weather=update_weather)
    weather = pd.DataFrame.from_dict(weather)
    weather.DATE = pd.to_datetime(weather.DATE, format='%Y-%m-%d')
    weather.TMAX = pd.to_numeric(weather.TMAX)
    weather.TMIN = pd.to_numeric(weather.TMIN)
    weather['HDD'] = 65 - ((weather.TMAX + weather.TMIN) / 2)
    weather.loc[weather.HDD < 0, 'HDD'] = 0
    weather['days'] = 1
    weather = weather.set_index(weather.DATE, drop=True)
    return weather


@timer
def calculate_results(reads, weather, piecewise=True, group_by='agl_premise_number'):
    return reads.groupby(group_by).apply(lambda x: scare_result(weather=weather, df=x, piecewise=piecewise))


@timer
def pivot_table(df):
    df = df.reset_index()
    return df.pivot(index='agl_premise_number',
                    columns='level_1',
                    values='Normalized').astype(float)


@timer
def main():
    reads = retrieve_reads()
    weather = retrieve_weather_and_clean(update_weather=True)
    reads = reads.rename(columns={'ccf': 'UsgCCF',
                                  'begin_date': 'StartDate',
                                  'end_date': 'EndDate'})
    grouped_results = calculate_results(reads, weather, group_by='agl_premise_number')
    df = pivot_table(grouped_results)
    return df


def test():
    weather = retrieve_weather_and_clean()
    gas_south_premises = query_premises.get_distinct_gas_south_premises(YEAR, MONTH)
    reads = pd.read_csv('test.csv')
    reads = reads.merge(gas_south_premises, how='inner',
                        left_on='agl_premise_number', right_on='aglservicelocationnumber')
    reads.begin_date = pd.to_datetime(reads.begin_date)
    reads.end_date = pd.to_datetime(reads.end_date)
    reads.ccf = pd.to_numeric(reads.ccf)
    reads = reads.rename(columns={'ccf': 'UsgCCF',
                                  'begin_date': 'StartDate',
                                  'end_date': 'EndDate'})

    grouped_results = calculate_results(reads, weather, group_by='agl_premise_number')
    grouped_results.to_csv('test_results.csv')
    return grouped_results
