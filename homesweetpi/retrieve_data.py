"""
Module for retrieving data from raspberry pi apis
"""

import os
import requests
import logging
import pandas as pd
from sql_tables import get_ip_addr, get_sensors_on_pi, get_last_time
from sql_tables import ENGINE, get_pi_ids
import sqlalchemy
import json
import time

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
    Returns a pandas datafraame
    """
    ipaddr = get_ip_addr(piid)
    strftime = query_time.strftime('%Y%m%d%H%M')
    url = f"http://{ipaddr}:{port}/get_recent/{strftime}"
    LOG.debug("fetching data from {}".format(url))
    response = requests.get(url)
    recent_data = response.json()
    recent_data = json.loads(recent_data)
    recent_data = pd.DataFrame(recent_data)
    recent_data['datetime'] = pd.to_datetime(recent_data['datetime'],
                                             unit="ms")
    sensors = get_sensors_on_pi(piid)
    recent_data = recent_data.merge(sensors, on=['location', 'piname'])\
                             .drop(["piname", "location", "sensortype",
                                    "piid"], axis=1)
    return recent_data


if __name__ == "__main__":
    set_up_python_logging(debug=True, log_filename="data_retrieval.log",
                          log_path="")
    LOG.debug("fetching pi ids")
    ids = get_pi_ids()
    LOG.debug("pi ids {}".format(ids))
    freq = 300
    LOG.debug("fetch frequency set to {} seconds".format(freq))
    while True:
        for piid in ids:
            query_time = get_last_time(piid)
            msg = "fetching data for pi {} since time {}".format(piid,
                                                                 query_time)
            LOG.debug(msg)
            recent_data = fetch_recent_data(piid, query_time)
            recent_data = recent_data.drop_duplicates(subset=['datetime',
                                                              'sensorid'])
            LOG.debug("saving fetched data to db")
            try:
                recent_data.to_sql("measurements", ENGINE, index=False,
                                   if_exists="append")
            except sqlalchemy.exc.IntegrityError as e:
                "Error on saving data for {} from {}: {}".format(piid,
                                                                 query_time,
                                                                 e)
                LOG.warning(msg)
        time.sleep(freq)
