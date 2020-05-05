"""
Module for retrieving data from raspberry pi apis
"""

import logging
import json
import time
from datetime import datetime, timedelta
import requests
import pandas as pd
from homesweetpi.sql_tables import get_ip_addr, get_sensors_on_pi,\
                                   get_last_time, SESSION
from homesweetpi.sql_tables import get_pi_ids, save_recent_data

LOG = logging.getLogger("homesweetpi.data_retrieval")


def process_fetched_data(recent_data, session=SESSION()):
    """
    Parse the json-like string fetched from the pi_logger api
    merge with the sensor information
    return as a pandas dataframe
    """
    recent_data = json.loads(recent_data)
    recent_data = pd.DataFrame(recent_data)
    LOG.debug("shape of fetched data is %s", recent_data.shape)
    recent_data['datetime'] = pd.to_datetime(recent_data['datetime'],
                                             unit="ms")
    assert recent_data['piid'].unique().size == 1
    pi_id = recent_data['piid'].unique()[0]
    sensors = get_sensors_on_pi(pi_id, session=session)
    LOG.debug("sensors on pi %s are %s", pi_id, sensors)
    recent_data = recent_data.merge(sensors, on=['location', 'piname'])\
                             .drop(["piname", "location", "sensortype",
                                    "piid"], axis=1)
    LOG.debug("shape of fetched data after merge is %s", recent_data.shape)
    recent_data = recent_data.drop_duplicates(subset=['datetime',
                                                      'sensorid'])
    recent_data = recent_data.drop(['id'], axis=1)
    LOG.debug("shape of data after drop duplicates %s", recent_data.shape)
    return recent_data


def fetch_recent_data(pi_id, query_time, session=SESSION(), port=5003):
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
    except (ConnectionError, ConnectionRefusedError) as error:
        LOG.debug("ConnectionError from %s: %s", ipaddr, error)
        msg = f'{{"message": "Could not connect to {ipaddr}"}}'
        recent_data = json.loads(msg)
    if len(recent_data) > 1:
        recent_data = process_fetched_data(recent_data, session)
        return recent_data

    LOG.debug("contents of json: %s. Returning None", recent_data)
    return None


def round_up_seconds(datetime_):
    """
    Round up at datetime instance to the next second and return
    """
    LOG.debug("Rounding datetime instance to nearest second")
    rounded = (datetime_.year, datetime_.month, datetime_.day, datetime_.hour,
               datetime_.minute, datetime_.second)
    return datetime(*rounded) + timedelta(seconds=1)


def run_data_retrieval_loop(freq=300):
    """
    Data retrieval main function.
    Attempts to retrieve data from each pi included in database at the
    specified frequency
    Parameters:
        freq (int): the data retrieval frequency in seconds
    """
    LOG.debug("fetching pi ids")
    ids = get_pi_ids()
    LOG.debug("pi ids %s", ids)
    LOG.debug("fetch frequency set to %s seconds", freq)
    while True:
        for piid in ids:
            qtime = get_last_time(piid)
            LOG.debug("most recent record in db for pi %s at %s", piid, qtime)
            qtime = round_up_seconds(qtime)
            LOG.debug("fetching data for pi %s since time %s", piid, qtime)
            recentdata = fetch_recent_data(piid, qtime)
            if recentdata is None:
                LOG.debug("No data to pass to sql: %s", recentdata)
            else:
                LOG.debug("saving fetched data to db: %s", recentdata)
                if not save_recent_data(recentdata):
                    LOG.warning("Error saving  pi %s from %s",
                                piid, qtime)
        time.sleep(freq)


if __name__ == "__main__":
    run_data_retrieval_loop(freq=300)
