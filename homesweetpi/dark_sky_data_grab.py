"""
Script for grabbing weather data from DarkSky API dumping to file
"""
import os
import json
import logging
from datetime import datetime, timedelta
import requests
import pandas as pd

LOG = logging.getLogger("homesweetpi.dark_sky_data_grab")

SECRET_KEY = os.getenv("DARKSKYKEY")
LOCATION = "TempelhoferFeld"
LATITUDE = '52.475752'  # Tempelhofer Feld
LONGITUDE = '13.407762'
TIMEZONE = 'CET'  # Central European Time
PATH = os.path.join(os.path.expanduser("~"),
                    "spiced", "data", "homesweetpi", "weather_data")


def get_weather_data(time_string):
    '''
    Query the DarkSky API for data for a given time_string
    Returns a json
    '''
    url_stem = 'https://api.darksky.net/forecast/{}/{},{},{}?units=si'
    url = url_stem.format(SECRET_KEY, LATITUDE, LONGITUDE, time_string)
    LOG.debug("requesting data for url %s", url)
    response = requests.get(url)
    return response.json()


def dump_to_json(json_obj, time_string, location=LOCATION, path=PATH,
                 filename_template="DarkSky_{}_{}.json"):
    """
    Dump a json object (json_obj) to file
    """
    filename = filename_template.format(location, time_string.replace(':', ''))
    filename = os.path.join(path, filename)
    LOG.debug("dumping json for time_string %s", time_string)
    with open(filename, 'w') as filepath:
        json.dump(json_obj, filepath)


def convert_to_df(weather_data, level='hourly', timezone=TIMEZONE):
    """
    Convert a json from the DarkSky API to a DataFrame using the specified time
    level
    Return a Pandas DataFrame
    """
    LOG.debug("converting %s data to dataframe", level)
    weather_data = pd.DataFrame(weather_data[level]['data'])
    datetime_series = pd.to_datetime(weather_data['time'], unit='s')
    weather_data['time'] = datetime_series.dt.tz_localize('UTC')\
                                          .dt.tz_convert(timezone)
    return weather_data


def dump_jsons_for_date_range(date_range,
                              location=LOCATION,
                              path=PATH,
                              filename_template="DarkSky_{}_{}.json"):
    """
    Fetch data for a range of times and dump jsons to file
    """
    time_strings = [dt.strftime('%Y-%m-%dT%H:%M:%S')
                    for dt in date_range]
    for time_t in time_strings:
        LOG.debug("requesting data for time_string %s", time_t)
        weather_data = get_weather_data(time_t)
        dump_to_json(weather_data, time_t, location=location, path=path,
                     filename_template=filename_template)


def get_dfs_for_date_range(date_range, path=PATH, level='hourly',
                           dump_json=True, location=LOCATION):
    """
    Fetch data for a range of times and add to a pandas dataframe
    """
    time_strings = [dt.strftime('%Y-%m-%dT%H:%M:%S')
                    for dt in date_range]
    list_of_dfs = []
    for time_t in time_strings:
        LOG.debug("requesting data for time_string %s", time_t)
        weather_data = get_weather_data(time_t)
        if dump_json:
            dump_to_json(weather_data, time_t, location=location, path=path,
                         filename_template="DarkSky_{}_{}.json")
        df_t = convert_to_df(weather_data, level=level, timezone=TIMEZONE)
        list_of_dfs.append(df_t)
    concatenated_data = pd.concat(list_of_dfs, sort=False)
    return concatenated_data


def main(first_datetime, last_datetime=None):
    """
    Grab data from the DarkSky API for every day up to the present
    Dump the json from each day to file and save the hourly data to a csv file
    Parameters:
        first (datetime.datetime): Beginning of the daterange to be grabbed.
        last (datetime.datetime): Last day of the daterange to be grabbed.
                                  Defaults to yesterdays date.
    """
    time_now = datetime.now()
    if last_datetime is None:
        today = datetime(time_now.year, time_now.month, time_now.day)
        yesterday = today - timedelta(days=1)
        last_datetime = yesterday
    date_range = pd.date_range(
        first_datetime, last_datetime,
        freq='d',
        tz=TIMEZONE,
    )
    # dump_jsons_for_date_range(date_range)
    str_format = '%Y%m%dT%H%M%S'
    csv_filename = "DarkSky_{}_{}-{}.csv".format(
        LOCATION,
        first_datetime.strftime(str_format),
        last_datetime.strftime(str_format),
    )
    csv_filepath = os.path.join(PATH, csv_filename)
    get_dfs_for_date_range(date_range).to_csv(csv_filepath, index=False)


if __name__ == "__main__":
    main(first_datetime=datetime(2019, 10, 8))
