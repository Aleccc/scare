# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 08:48:11 2019

@author: GAS01409

https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation
"""

import os
import csv
import datetime
import requests
import pandas as pd

STATIC_WEATHER = os.path.join('weather', 'static', 'weather.csv')
STR_FORMAT = '%Y-%m-%d'
START = '1991-12-01'
END = datetime.datetime.today().strftime(STR_FORMAT)
STATIONS = ['USW00013874',  # ATL (&SNG)
            ]


def call_api():
    url = ('https://www.ncei.noaa.gov/access/services/data/v1?'
           'dataset=daily-summaries'
           '&dataTypes=TMAX,TMIN'
           '&stations={stations}'
           '&includeStationName=true'
           '&startDate={start}&endDate={end}'
           '&units=standard'
           '&format=json')
    url = url.format(stations=','.join(STATIONS),
                     start=START,
                     end=END)

    response = requests.get(url)
    return response.json()


def get_static_data():
    if os.path.exists(STATIC_WEATHER):
        with open(STATIC_WEATHER, 'r') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        return data


def max_date(data_list):
    return max([sublist['DATE'] for sublist in data_list])


def add_one_day(date):
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, STR_FORMAT)
    date += datetime.timedelta(1)
    return date.strftime(STR_FORMAT)


def append_data(response):
    first_time = not os.path.exists(STATIC_WEATHER)
    with open(STATIC_WEATHER, 'a', newline='') as f:
        fieldnames = response[0].keys()
        writer = csv.DictWriter(f, fieldnames)
        if first_time:
            writer.writeheader()
        writer.writerows(response)


def _calculate_hdd(data):
    weather = pd.DataFrame.from_dict(data)
    weather.DATE = pd.to_datetime(weather.DATE, format='%Y-%m-%d')
    weather.TMAX = pd.to_numeric(weather.TMAX)
    weather.TMIN = pd.to_numeric(weather.TMIN)
    weather['HDD'] = 65 - ((weather.TMAX + weather.TMIN) / 2)
    weather.loc[weather.HDD < 0, 'HDD'] = 0
    weather['days'] = 1
    weather = weather.set_index(weather.DATE, drop=True)
    return weather


def get_weather(update_weather=False):
    data = get_static_data()
    if update_weather:
        try:
            latest_date = max_date(data)
        except TypeError:  # file is empty
            latest_date = None
        if latest_date:
            if latest_date >= END:
                return data
            else:
                global START
                START = add_one_day(latest_date)
                json_response = call_api()
                append_data(json_response)
                data = get_static_data()
        else:
            json_response = call_api()
            append_data(json_response)
            data = get_static_data()
    return _calculate_hdd(data)
