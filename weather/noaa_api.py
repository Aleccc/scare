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

STATIC_WEATHER = os.path.join('weather', 'static', 'weather.csv')
STR_FORMAT = '%Y-%m-%d'
START = '1999-01-01'
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
        writer.writerows(response.json())


def get_data(update_weather=False):
    data = get_static_data()
    if update_weather:
        latest_date = max_date(data)
        if latest_date >= END:
            return data
        else:
            global START
            if latest_date:  # in case latest_date == None
                START = add_one_day(latest_date)
            call_api()
            data = get_static_data()
    return data
