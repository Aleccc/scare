import pandas as pd
import dask.dataframe as dd

from datawarehouse.queries import query_premises
from weather.noaa_api import get_weather
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
def retrieve_reads_from_file(filter_top_n=None):
    reads = pd.read_csv('reads.csv')
    reads.agl_premise_number = pd.to_numeric(reads.agl_premise_number)
    reads.begin_date = pd.to_datetime(reads.begin_date)
    reads.end_date = pd.to_datetime(reads.end_date)
    reads.ccf = pd.to_numeric(reads.ccf)

    if filter_top_n:
        try:
            premises = list(set(reads.agl_premise_number))[:filter_top_n]
            reads = reads[reads.agl_premise_number.isin(premises)]
        except TypeError as e:
            print('Type Error: filter_top_n must be an integer')
    return reads


@timer
def calculate_results(reads, weather, group_by='agl_premise_number'):
    return reads.groupby(group_by).apply(lambda x: scare_result(weather=weather, df=x))


@timer
def pivot_table(df):
    df = df.reset_index()
    return df.pivot(index='agl_premise_number',
                    columns='level_1',
                    values='Normalized').astype(float)


def func_for_dask(df):
    return scare_result(weather=WEATHER, df=df)


@timer  # 29343.66sec
def main(reads):
    readds = dd.from_pandas(reads, npartitions=8)
    expected = pd.DataFrame(columns=['Normalized', ], dtype=float)
    results = readds.groupby('agl_premise_number').apply(func_for_dask, meta=expected).compute(scheduler='processes')
    df = pivot_table(results)
    return df


if __name__ == "__main__":
    WEATHER = get_weather(update_weather=True)
    READ_FROM_FILE = True
    if READ_FROM_FILE:
        all_reads = retrieve_reads_from_file(filter_top_n=1000)
    else:
        all_reads = retrieve_reads()
    all_reads = all_reads.rename(columns={'ccf': 'UsgCCF',
                                          'begin_date': 'StartDate',
                                          'end_date': 'EndDate'})
    real = main(all_reads)
    # real.to_csv('multi_full_run.csv')

    # premises = 1000, pandas apply: 193.72
    # premises = 1000, n = 4, processes: 78.15
    # premises = 1000, n = 6, processes: 74.79
    # premises = 1000, n = 8, processes: 74.45
    # premises = 1000, n = 16, processes: 83.71
