"""
Module for retrieving data from raspberry pi apis
"""

import os
import requests
from requests.exceptions import ConnectionError
import logging
import pandas as pd
from sql_tables import get_ip_addr, get_sensors_on_pi, get_last_time
from sql_tables import ENGINE, get_pi_ids
import sqlalchemy
import json
import time
from datetime import datetime, timedelta

LOG = logging.getLogger(f'data_fetch')


def set_up_python_logging(debug=False,
                          log_filename="local_loggers.log",
                          log_path=""):
    """
    Set up the python logging module
    """
    log_filename = os.path.join(log_path, log_filename)
    handler = logging.FileHandler(log_filename, mode='a')
    fmt = '%(asctime)s %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'
    handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    LOG.addHandler(handler)
    if debug:
        # logging.basicConfig(filename=filename, level=logging.DEBUG)
        LOG.setLevel(logging.DEBUG)
    else:
        # logging.basicConfig(filename=filename, level=logging.INFO)
        LOG.setLevel(logging.INFO)


def fetch_recent_data(piid, query_time, port=5002):
    """
    Get all data since query_time from a raspberry pi identified by piid
    Returns a pandas dataframe
    """
    ipaddr = get_ip_addr(piid)
    strftime = query_time.strftime('%Y%m%d%H%M%S')
    url = f"http://{ipaddr}:{port}/get_recent/{strftime}"
    LOG.debug(f"fetching data from {url}")
    try:
        response = requests.get(url)
        recent_data = response.json()
        LOG.debug(f"recieved json with length {len(recent_data)}")
    except ConnectionError as e:
        LOG.debug(f"ConnectionError from {ipaddr}: {e}")
        msg = f'{{"message": "Could not connect to {ipaddr}"}}'
        recent_data = json.loads(msg)
    if len(recent_data) > 1:
        recent_data = json.loads(recent_data)
        recent_data = pd.DataFrame(recent_data)
        LOG.debug(f"shape of fetched data is {recent_data.shape}")
        recent_data['datetime'] = pd.to_datetime(recent_data['datetime'],
                                                 unit="ms")
        sensors = get_sensors_on_pi(piid)
        LOG.debug(f"sensors on pi {piid} are {sensors}")
        recent_data = recent_data.merge(sensors, on=['location', 'piname'])\
                                 .drop(["piname", "location", "sensortype",
                                        "piid"], axis=1)
        LOG.debug(f"shape of fetched data after merge is {recent_data.shape}")

        recent_data = recent_data.drop_duplicates(subset=['datetime',
                                                          'sensorid'])
        LOG.debug(f"shape of data after drop duplicates {recent_data.shape}")
        return recent_data
    else:
        LOG.debug(f"contents of json: {recent_data}")


def round_up_seconds(datetime_):
    """
    Round up at datetime instance to the next second and return
    """
    dt = datetime_
    rounded = dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
    return datetime(*rounded) + timedelta(seconds=1)


if __name__ == "__main__":
    set_up_python_logging(debug=True, log_filename="data_retrieval.log",
                          log_path="")
    LOG.debug("fetching pi ids")
    ids = get_pi_ids()
    LOG.debug(f"pi ids {ids}")
    freq = 300
    LOG.debug(f"fetch frequency set to {freq} seconds")
    while True:
        for piid in ids:
            query_time = get_last_time(piid)
            msg = f"most recent record in db for pi {piid} at {query_time}"
            LOG.debug(msg)
            query_time = round_up_seconds(query_time)
            msg = f"fetching data for pi {piid} since time {query_time}"
            LOG.debug(msg)
            recent_data = fetch_recent_data(piid, query_time)
            if recent_data is None:
                LOG.debug("No data to pass to sql")
            else:
                LOG.debug("saving fetched data to db")
                try:
                    recent_data.to_sql("measurements", ENGINE, index=False,
                                       if_exists="append")
                except sqlalchemy.exc.IntegrityError as e:
                    msg = f"Error saving  pi {piid} from {query_time}: {e}"
                    LOG.warning(msg)
        time.sleep(freq)
