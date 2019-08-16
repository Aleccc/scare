# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 11:27:33 2018

@author: gas01409
"""
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
        pool = df.delivery_group.iloc[0]
        self.df['UsgDTH'] = self.df.UsgCCF * BTU_FACTOR[pool] / 10
        self.weather_monthly = self._weather2monthly()
        self.usage_daily = self._convert_usage2daily()
        self.usage_monthly = self._calendarize()

    def _convert_usage2daily(self):
        """Create a series that indexes off of daily reads, assigning average
        dathm to each day between the StartDate and EndDate of the ReadMonth.

        2019-05-29    0.12384
        2019-05-30    0.12384
        2019-05-31    0.12384
        2019-06-01    0.12384
        2019-06-02    0.12384
        2019-06-03    0.12384
        dtype: float64
        """
        full = pd.Series()
        for i in range(len(self.df)):
            index = pd.date_range(self.df.StartDate.iloc[i],
                                  self.df.EndDate.iloc[i]-timedelta(1))
            series = pd.Series(1, index=index)  # assign each day a value of 1
            daily_avg_dathm = self.df.UsgDTH.iloc[i]/series.sum()
            full = full.append(series*daily_avg_dathm)
        return full

    def _calendarize(self):
        """Merge (left join) the full daily usage with the daily weather.
        Sum each by month: usage, HDDs, and days.
        Include new column 'month' to indicate month (e.g. Jan = 1).

                         dth  TMAX  TMIN  HDD  days   year  month
        2019-05-31  2.724480  1917  1493  3.5    22  44418      5
        2019-06-30  2.485341  2582  2081  0.0    30  60570      6
        2019-07-31  0.156579   188   149  0.0     2   4038      7
        """
        daily = pd.DataFrame(self.usage_daily, columns=['dth'])
        merged = daily.merge(self.weather_daily, left_index=True, right_index=True)
        monthly = merged.resample('M').sum()  # 'M' = monthly
        monthly['month'] = monthly.index.month  # Jan=1, Feb=2,...
        return monthly

    def _weather2monthly(self):
        """Prepare past weather data for prediction by grouping by month

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
        return weather_monthly

    def piecewise(self, gripm=False):
        """Piecewise Regression that segments different months"""
        # do four regressions with the following segmented months
        if gripm:
            segments = [[12, 1, 2],   # Dec-Feb
                        [2, 3, 4],    # Feb-Apr
                        [4, 10, 11],  # Apr,Oct,Nov
                        [5, 6, 9]]    # May,Jun,Sep
            # assign the results of above segments to the following months
            assigns = [[12, 1, 2],
                       [3],
                       [4, 10, 11],
                       [5, 6, 7, 8, 9]]  # 7 and 8 replaced with actual daily averages
        else:
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
        for i in range(4):
            seg = self.usage_monthly.index.month.isin(segments[i])
            lm = smf.ols("dth~HDD+days+0", data=self.usage_monthly[seg]).fit()
            # lm.summary()
            seg_weather = self.weather_monthly.index.get_level_values("Month").isin(assigns[i])
            segment_prediction = lm.predict(self.weather_monthly[seg_weather])
            segment_prediction = pd.DataFrame(segment_prediction, columns=["dathm"])
            prediction = prediction.append(segment_prediction)
        prediction = prediction.sort_index()
        prediction[prediction < 0] = 0  # set negative predictions to zero
        return prediction

    def simple(self):
        """Simple Regression on HDDs and days"""
        lm = smf.ols("dth~HDD+days+0", data=self.usage_monthly).fit()
        # lm.summary()
        prediction = lm.predict(self.weather_monthly)
        prediction = pd.DataFrame(prediction, columns=["dathm"])
        prediction.loc[prediction.dathm < 0, 'dathm'] = 0  # set negative predictions to zero
        return prediction


def statistics(predictions, num_years=10, plot=False):
    """Print the monthly Average, Standard Deviation, and Skewness
    across the recent requested number of years (num_years)"""
    years = range(predictions.index.max()[0]-num_years,
                  predictions.index.max()[0]-1)
    stats = predictions.loc[years].groupby(level=1).mean()
    stats = stats.rename(columns={"dathm": "Normalized"})
    # stats['std'] = predictions.loc[years].groupby(level=1).std()
    # stats['skew'] = predictions.loc[years].groupby(level=1).skew()
    # if plot:
    #     stats.Normalized.plot(title="%d-Year Normalized Average" % num_years,
    #                           legend=True, years=num_years)
    #     print(stats)
    return stats


def scare_result(weather, df, piecewise=True):
    obj = SCARE(weather, df=df)
    try:
        if piecewise:
            prediction = obj.piecewise()
        else:
            prediction = obj.simple()
        # Statistics of recent 10 years
        stats = statistics(prediction, num_years=10)
        if piecewise:
            # Pricing model replaces July (7) and August (8) with actual daily average
            chg = obj.usage_daily
            new = chg[(chg.index.month == 7) | (chg.index.month == 8)].mean()*31
            stats.loc[7, 'Normalized'] = new
            stats.loc[8, 'Normalized'] = new
    except:
        billing_average = pd.DataFrame(index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                                       data=obj.usage_monthly.groupby('month').dth.mean())
        billing_average = billing_average.rename(columns={'dth': 'Normalized'})
        billing_average = billing_average.fillna(0)  # replaces any missing months with zero
        # TODO: instead of filling with zero, give a generic full shape
        stats = billing_average

    checks = pd.DataFrame.from_dict({'check': ['test1', 'test2', 'test3'],
                                     'Normalized': [test_one(stats), test_two(stats), test_three(stats)],
                                     })
    checks = checks.set_index('check')
    stats = stats.append(checks)
    return stats
