import pandas as pd
from datawarehouse.queries import query_premises
from weather.noaa_api import get_data
from scare.scare import scare_result
from timer.timer import timer

YEAR = 2019
MONTH = 8


@timer
def retrieve_reads():
    """Return two years worth of meter reads for only Gas South customers.
    get_reads_for_gas_south_customers time: 90.17sec
    get_reads_for_gas_south_customers time: 82.91sec (year_prior=True)
    reads.to_csv time: 123.16sec
    """
    reads = query_premises.get_reads_for_gas_south_customers_on_premise(YEAR, MONTH)
    reads = reads.append(query_premises.get_reads_for_gas_south_customers_on_premise(YEAR, MONTH, year_prior=True))
    # # export as csv
    # reads.to_csv('reads.csv', index=False)
    reads = reads.reset_index(drop=True)
    return reads


@timer
def retrieve_weather_and_clean(update_weather=False):
    """retrieve_weather_and_clean time: 0.08sec"""
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
def calculate_results(reads, weather, piecewise, group_by='agl_premise_number'):
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
    weather = retrieve_weather_and_clean(update_weather=False)
    reads = reads.rename(columns={'ccf': 'UsgCCF',
                                  'begin_date': 'StartDate',
                                  'end_date': 'EndDate'})
    grouped_results = calculate_results(reads, weather, piecewise=True, group_by='agl_premise_number')
    df = pivot_table(grouped_results)
    return df


def test():
    weather = retrieve_weather_and_clean()
    # reads = retrieve_reads()
    reads = pd.read_csv('reads.csv')
    nic = pd.read_csv('nic.csv')
    nic.agl_premise_number = pd.to_numeric(nic.agl_premise_number)
    reads.agl_premise_number = pd.to_numeric(reads.agl_premise_number)
    reads.begin_date = pd.to_datetime(reads.begin_date)
    reads.end_date = pd.to_datetime(reads.end_date)
    reads.ccf = pd.to_numeric(reads.ccf)
    reads = reads.rename(columns={'ccf': 'UsgCCF',
                                  'begin_date': 'StartDate',
                                  'end_date': 'EndDate'})

    these = reads.merge(nic, left_on='agl_premise_number', right_on='agl_premise_number')
    grouped_results = calculate_results(these, weather, piecewise=True, group_by='agl_premise_number')
    df = pivot_table(grouped_results)
    return df


# res = test()
# res.to_csv('for_nic2.csv')
real = main()
real.to_csv('second_full_run.csv')
