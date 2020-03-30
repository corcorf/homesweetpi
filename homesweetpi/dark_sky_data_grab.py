import os
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.DEBUG)

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
    logging.debug(f"requesting data for url {url}")
    response = requests.get(url)
    return response.json()


def dump_to_json(weather_data, time_string, location=LOCATION, path=PATH,
                 fn_template="DarkSky_{}_{}.json"):
    fn = fn_template.format(location, time_string.replace(':', ''))
    fn = os.path.join(path, fn)
    logging.debug(f"dumping json for time_string {time_string}")
    with open(fn, 'w') as fp:
        json.dump(weather_data, fp)


def convert_to_df(weather_data, level='hourly', tz=TIMEZONE):
    """
    Convert a json from the DarkSky API to a DataFrame using the specified time
    level
    Return a Pandas DataFrame
    """
    logging.debug(f"converting {level} data to dataframe")
    df = pd.DataFrame(weather_data[level]['data'])
    dt = pd.to_datetime(df['time'], unit='s')
    df['time'] = dt.dt.tz_localize('UTC').dt.tz_convert(tz)
    return df


def dump_jsons_for_date_range(date_range,
                              location=LOCATION,
                              path=PATH,
                              fn_template="DarkSky_{}_{}.json"):
    """
    Fetch data for a range of times and dump jsons to file
    """
    time_strings = [dt.strftime('%Y-%m-%dT%H:%M:%S')
                    for dt in date_range]
    for t in time_strings:
        logging.debug(f"requesting data for time_string {t}")
        weather_data = get_weather_data(t)
        dump_to_json(weather_data, t, location=location, path=path,
                     fn_template=fn_template)


def get_dfs_for_date_range(date_range, path=PATH, level='hourly',
                           dump_json=True, location=LOCATION):
    """
    Fetch data for a range of times and add to a pandas dataframe
    """
    time_strings = [dt.strftime('%Y-%m-%dT%H:%M:%S')
                    for dt in date_range]
    list_of_dfs = []
    for t in time_strings:
        logging.debug(f"requesting data for time_string {t}")
        weather_data = get_weather_data(t)
        if dump_json:
            dump_to_json(weather_data, t, location=location, path=path,
                         fn_template="DarkSky_{}_{}.json")
        df = convert_to_df(weather_data, level=level, tz=TIMEZONE)
        list_of_dfs.append(df)
    df = pd.concat(list_of_dfs, sort=False)
    return df


if __name__ == "__main__":
    dt = datetime.now()
    today = datetime(dt.year, dt.month, dt.day)
    yesterday = today - timedelta(days=1)
    first = datetime(2019, 10, 8)
    date_range = pd.date_range(first, yesterday, freq='d', tz=TIMEZONE)
    # dump_jsons_for_date_range(date_range)
    fn = "DarkSky_{}_{}-{}.csv".format(LOCATION,
                                       first.strftime('%Y%m%dT%H%M%S'),
                                       yesterday.strftime('%Y%m%dT%H%M%S'))
    fp = os.path.join(PATH, fn)
    get_dfs_for_date_range(date_range).to_csv(fp, index=False)
