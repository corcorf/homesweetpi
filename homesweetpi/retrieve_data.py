"""
Module for retrieving data from raspberry pi apis
"""

import os
import logging
import json
import time
from datetime import datetime, timedelta
import requests
import pandas as pd
import sqlalchemy
from sql_tables import get_ip_addr, get_sensors_on_pi, get_last_time
from sql_tables import ENGINE, get_pi_ids

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


def fetch_recent_data(pi_id, query_time, port=5002):
    """
    Get all data since query_time from a raspberry pi identified by pi_id
    Returns a pandas dataframe
    """
    ipaddr = get_ip_addr(pi_id)
    strftime = query_time.strftime('%Y%m%d%H%M%S')
    url = f"http://{ipaddr}:{port}/get_recent/{strftime}"
    LOG.debug("fetching data from %s", url)
    try:
        response = requests.get(url)
        recent_data = response.json()
        LOG.debug("recieved json with length %s", len(recent_data))
    except ConnectionError as error:
        LOG.debug("ConnectionError from %s: %s", ipaddr, error)
        msg = f'{{"message": "Could not connect to {ipaddr}"}}'
        recent_data = json.loads(msg)
    if len(recent_data) > 1:
        recent_data = json.loads(recent_data)
        recent_data = pd.DataFrame(recent_data)
        LOG.debug("shape of fetched data is %s", recent_data.shape)
        recent_data['datetime'] = pd.to_datetime(recent_data['datetime'],
                                                 unit="ms")
        sensors = get_sensors_on_pi(pi_id)
        LOG.debug("sensors on pi %s are %s", pi_id, sensors)
        recent_data = recent_data.merge(sensors, on=['location', 'piname'])\
                                 .drop(["piname", "location", "sensortype",
                                        "pi_id"], axis=1)
        LOG.debug("shape of fetched data after merge is %s", recent_data.shape)

        recent_data = recent_data.drop_duplicates(subset=['datetime',
                                                          'sensorid'])
        LOG.debug("shape of data after drop duplicates %s", recent_data.shape)
        return recent_data

    LOG.debug("contents of json: %s", recent_data)
    return None


def round_up_seconds(datetime_):
    """
    Round up at datetime instance to the next second and return
    """
    rounded = (datetime_.year, datetime_.month, datetime_.day, datetime_.hour,
               datetime_.minute, datetime_.second)
    return datetime(*rounded) + timedelta(seconds=1)


if __name__ == "__main__":
    set_up_python_logging(debug=True, log_filename="data_retrieval.log",
                          log_path="")
    LOG.debug("fetching pi ids")
    IDS = get_pi_ids()
    LOG.debug("pi ids %s", IDS)
    FREQ = 300
    LOG.debug("fetch frequency set to %s seconds", FREQ)
    while True:
        for piid in IDS:
            qtime = get_last_time(piid)
            LOG.debug("most recent record in db for pi %s at %s", piid, qtime)
            qtime = round_up_seconds(qtime)
            LOG.debug("fetching data for pi %s since time %s", piid, qtime)
            recentdata = fetch_recent_data(piid, qtime)
            if recentdata is None:
                LOG.debug("No data to pass to sql")
            else:
                LOG.debug("saving fetched data to db")
                try:
                    recentdata.to_sql("measurements", ENGINE, index=False,
                                      if_exists="append")
                except sqlalchemy.exc.IntegrityError as error:
                    LOG.warning("Error saving  pi %s from %s: %s",
                                piid, qtime, error)
        time.sleep(FREQ)
