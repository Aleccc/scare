# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 11:27:33 2018

@author: gas01409
"""
import logging
from datetime import timedelta
import pandas as pd
import statsmodels.formula.api as smf
from .checks import test_one, test_two, test_three

BTU_FACTOR = {'ATL': 1.032,
              'SNG': 1.017,
              'TRA': 1.025,
              'AUG': 1.033,
              'SAV': 1.031,
              'ROM': 1.017,
              'MAC': 1.031,
              'BRU': 1.033,
              'VAL': 1.033,
              }


class SCARE:
    """Calendarize reads and prepares weather data for normalization."""
    def __init__(self, weather, df):
        self.weather_daily = weather
        self.df = df
        self.trailing_months = 120  # for predictions (aka 10-year)
        pool = df.delivery_group.iloc[0]
        self.df['UsgDTH'] = self.df.UsgCCF * BTU_FACTOR[pool] / 10
        self.weather_monthly = self._weather2monthly()
        self.usage_daily = self._convert_usage2daily()
        self.usage_monthly = self._calendarize()
        self.summer_base_load_per_day = self._calculate_base_load()
        # adjust the consumption
        self.adjusted_usage_monthly = self._adjust_consumption()

    def _convert_usage2daily(self):
        """Create a dataframe that indexes off of daily reads, assigning average
        dathm to each day between the StartDate and EndDate of the ReadMonth.

                    meter_read     dathm
        2019-06-13           0  0.419250
        2019-06-14           0  0.419250
        2019-06-15           0  0.419250
        2019-06-16           0  0.419250
        2019-06-17           0  0.419250
        """
        full = pd.DataFrame()
        for i in range(len(self.df)):
            # create index of daily date range, excluding end date
            index = pd.date_range(self.df.StartDate.iloc[i],
                                  self.df.EndDate.iloc[i] - timedelta(1))
            meter_read = pd.Series(i, index=index)
            days_in_read = meter_read.count()
            dathm_in_read = self.df.UsgDTH.iloc[i]
            daily_avg_dathm = dathm_in_read/days_in_read
            temp = pd.DataFrame(meter_read, columns=['meter_read'])
            temp['dathm'] = daily_avg_dathm
            full = full.append(temp)
        return full  # DataFrame

    def _calendarize(self):
        """Merge (left join) the full daily usage with the daily weather.
        Sum each by month: usage, HDDs, and days.

               meter_read       dathm    HDD  days
        month                                     
        1               5  391.334400  402.5    18
        1               6  158.895750  190.0    13
        1              17  506.970000  448.0    20
        1              18  319.748000  314.5    11
        2               4  202.129655  194.0    16
        """
        daily = self.usage_daily
        merged = daily.merge(self.weather_daily, left_index=True, right_index=True)
        # gripm equally weights averages of two meter reads for one month
        # e.g. read A has daily average of 4 dathm per day and contains two days in February,
        #      read B has daily average of 16 dathm per day and contains 26 days in February,
        #      the response to HDD from two reads will equally affect the regression.
        group = merged.groupby([merged.index.month, merged.meter_read])
        monthly = group.agg({'dathm': 'sum',
                             'HDD': 'sum',
                             'days': 'sum'})
        # reset the multi-index so that it becomes it's own columns
        # then rename the 'month' from the default 'level_0'
        # then set 'month' as the index
        monthly = monthly.reset_index()\
            .rename(columns={'level_0': 'month'})\
            .set_index('month')
        return monthly  # DataFrame

    def _adjust_consumption(self):
        monthly = self.usage_monthly
        by_read = pd.DataFrame(monthly.groupby('meter_read')[['days', 'dathm', 'HDD']].sum())
        by_read = by_read.rename(columns={'days': 'read_days', 'dathm': 'read_dathm', 'HDD': 'read_HDD'})
        monthly = monthly.merge(by_read, left_on='meter_read', right_index=True)
        monthly['base_load'] = monthly.days * self.summer_base_load_per_day
        monthly['HDD_portion'] = monthly.HDD / monthly.read_HDD
        monthly.HDD_portion = monthly.HDD_portion.fillna(0)
        monthly['heat_response'] = monthly.HDD_portion * (monthly.read_dathm - self.summer_base_load_per_day*monthly.read_days)
        monthly.loc[monthly.heat_response < 0, 'heat_response'] = 0

        # if base_load is less than dathm, use base_load
        monthly['adj_dathm'] = monthly.dathm
        base_less_than_dathm = monthly.dathm > monthly.base_load
        monthly.loc[base_less_than_dathm, 'adj_dathm'] = monthly.loc[base_less_than_dathm, 'base_load']
        # add heat response
        monthly.adj_dathm += monthly.heat_response
        # if read_HDD is not zero, then overwrite dathm
        monthly.loc[monthly.read_HDD != 0, 'dathm'] = monthly.loc[monthly.read_HDD != 0, 'adj_dathm']
        return monthly

    def _weather2monthly(self):
        """Prepare past weather data for prediction by grouping by month.
        Will only keep the months in the trailing_months window

                      HDD  days  month
        Year Month
        1999 1      526.5    31      1
             2      427.0    28      2
             3      452.0    31      3
             4       95.5    30      4
             5        8.5    31      5
                   ...   ...    ...
        2019 4       82.0    30      4
             5        3.5    31      5
             6        0.0    30      6
             7        0.0    31      7
             8        0.0    11      8
        [248 rows x 3 columns]
        """
        self.weather_daily['year'] = self.weather_daily.index.year
        self.weather_daily['month'] = self.weather_daily.index.month
        weather_monthly = self.weather_daily.groupby(['year', 'month'])
        weather_monthly = weather_monthly['HDD', 'days'].sum()
        weather_monthly['month'] = [x[1] for x in weather_monthly.index]
        weather_monthly.index.names = ['Year', 'Month']

        month_start = (self.trailing_months+1) * (-1)
        month_end = -1  # do not include current month, since not complete
        weather_monthly = weather_monthly.iloc[month_start:month_end]
        return weather_monthly  # DataFrame

    def _calculate_base_load(self):
        try:
            summer = self.usage_monthly.loc[[7, 8]]
            by_month = summer.groupby(summer.index).sum()
            month_average = by_month.dathm / by_month.days
            return month_average.mean()  # Float
        except KeyError:  # "None of [Int64Index([7, 8], dtype='int64', name='month')] are in the [index]"
            return 0

    def regression(self):
        """Piecewise Regression that segments different months"""
        # do four regressions with the following segmented months
        segments = [[1, 2],        # Jan, Feb
                    [3, 4],        # Mar, Apr
                    [10, 11, 12],  # Oct-Dec
                    [5, 6, 9]]     # May, Jun, Sep
        # assign the results of above segments to the following months
        assigns = [[12, 1, 2],
                   [3, 4],
                   [10, 11],
                   [5, 6, 7, 8, 9]]  # 7 and 8 replaced with actual daily averages
        prediction = pd.DataFrame()
        for i in range(len(segments)):
            seg = self.adjusted_usage_monthly.index.isin(segments[i])
            lm = smf.ols("dathm~HDD+days+0", data=self.adjusted_usage_monthly[seg]).fit()
            logging.debug('segments %s', segments[i])
            logging.debug(lm.summary())
            seg_weather = self.weather_monthly.index.get_level_values("Month").isin(assigns[i])
            segment_prediction = lm.predict(self.weather_monthly[seg_weather])
            segment_prediction = pd.DataFrame(segment_prediction, columns=["dathm"])
            prediction = prediction.append(segment_prediction)
        prediction = prediction.sort_index()
        prediction[prediction < 0] = 0  # set negative predictions to zero
        return prediction

    def billing_averages(self):
        billing_daily = self.usage_monthly.dathm / self.usage_monthly.days
        days_in_months = pd.DataFrame(index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                                      data=[31, 28.25, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
                                      columns=['Normalized'])
        billing_average = pd.DataFrame(index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                                       data=billing_daily.groupby('month').mean(),
                                       columns=['Normalized']) * days_in_months
        billing_average = billing_average.fillna(0)  # replaces any missing months with zero
        # TODO: instead of filling with zero, give a generic full shape
        return billing_average


def statistics(predictions, trailing_months=120, plot=False):
    """Print the monthly Average"""
    stats = predictions.groupby(level=1).mean()
    stats = stats.rename(columns={"dathm": "Normalized"})
    return stats


def scare_result(weather, df):
    using_scare = True
    total_days_of_reads = (df.EndDate - df.StartDate).sum().days
    if total_days_of_reads == 0:
        stats = pd.DataFrame.from_dict({'Normalized': [None, ]})
        using_scare = False
    else:
        obj = SCARE(weather, df=df)
        try:
            prediction = obj.regression()
            # Statistics of recent 10 years
            stats = statistics(prediction, trailing_months=120)
            # Pricing model replaces July (7) and August (8) base_load
            new = obj.summer_base_load_per_day*31
            stats.loc[7, 'Normalized'] = new
            stats.loc[8, 'Normalized'] = new
            billing_annual_sum = obj.billing_averages().sum().iloc[0]
            prediction_annual_sum = stats.Normalized.sum()
            if 0.3 < abs(1-(billing_annual_sum/prediction_annual_sum)):
                # if the absolute difference between the prediction and billing loads
                # are greater than 30%, then use the billing load
                stats = obj.billing_averages()
                using_scare = False
        except ValueError:
            stats = obj.billing_averages()
            using_scare = False

    checks = pd.DataFrame.from_dict({'check': ['test1', 'test2', 'test3',
                                               'using_scare', 'read_count'],
                                     'Normalized': [test_one(stats), test_two(stats), test_three(stats),
                                                    using_scare, len(df)],
                                     })
    checks = checks.set_index('check')
    stats = stats.append(checks)
    return stats
