#!/usr/bin/env python

"""
Tests for homesweetpi's sql_tables module.
Creates an SQLite db instead of the usual PostGres
"""

import os
import pytest
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from homesweetpi.sql_tables import create_tables, load_sensor_and_pi_info,\
                                   get_pi_names, get_sensor_locations,\
                                   get_last_time, save_recent_data,\
                                   one_or_more_results, Measurement,\
                                   get_measurements_since, get_last_n_days
from homesweetpi.retrieve_data import process_fetched_data

TEST_TIME = datetime.now()
TEST_DB_PATH = os.getcwd()
TEST_DB_FILENAME = "test_{}.db".format(TEST_TIME.strftime("%Y%m%d_%H%M%S"))
TEST_DB_FILEPATH = os.path.join(TEST_DB_PATH, TEST_DB_FILENAME)
CONN_STRING = f'sqlite:///{TEST_DB_FILEPATH}'
ENGINE = create_engine(CONN_STRING, echo=False)
SESSION = sessionmaker(bind=ENGINE)

PI_FILE = "pi_ip.csv"
SENSOR_FILE = "logger_config.csv"
PI_INFO = pd.read_csv(PI_FILE)
SENSOR_CONFIG = pd.read_csv(SENSOR_FILE)

SAMPLE_JSON = "{\"datetime\":{\"0\":1585930980235,\"1\":1585930980781},\
\"location\":{\"0\":\"livingroom\",\"1\":\"piano\"},\
\"sensortype\":{\"0\":\"dht22\",\"1\":\"dht22\"},\
\"piname\":{\"0\":\"catflap\",\"1\":\"catflap\"},\
\"piid\":{\"0\":\"100000003d12f229\",\"1\":\"100000003d12f229\"},\
\"temp\":{\"0\":19.0,\"1\":19.1000003815},\
\"humidity\":{\"0\":51.9000015259,\"1\":51.0999984741},\
\"pressure\":{\"0\":null,\"1\":null},\
\"gasvoc\":{\"0\":null,\"1\":null}}"


def test_create_db():
    """Test creating the tables in SQLite DB"""
    create_tables(ENGINE)
    assert os.path.exists(TEST_DB_FILEPATH)


def test_add_pi_info():
    """add sensor info from file to test.db"""
    load_sensor_and_pi_info(PI_FILE, SENSOR_FILE, engine=ENGINE)
    pi_names_from_db = get_pi_names(session=SESSION())
    pi_names_from_db.sort()
    assert isinstance(pi_names_from_db, np.ndarray)
    pi_names_from_file = PI_INFO['name'].values
    pi_names_from_file.sort()
    assert np.all(pi_names_from_db == pi_names_from_file)
    sensor_locations_from_db = get_sensor_locations(session=SESSION())
    assert isinstance(sensor_locations_from_db, np.ndarray)
    sensor_locations_from_file = SENSOR_CONFIG['location'].values
    sensor_locations_from_file.sort()
    assert np.all(sensor_locations_from_db == sensor_locations_from_file)


def test_json_processing():
    """
    check that sample json can be processed and transformed into a dataframe
    """
    recent_data = SAMPLE_JSON
    data_df = process_fetched_data(recent_data, session=SESSION())
    assert isinstance(data_df, pd.DataFrame)
    assert 'datetime' in data_df.columns


def test_no_results():
    """
    Check that the program correctly handles queries that produce no results
    """
    unlikely_date = datetime(2099, 1, 1, 0, 0)
    session = SESSION()
    query = session.query(Measurement)\
                   .filter(getattr(Measurement, "datetime") >= unlikely_date)
    assert not one_or_more_results(query)
    logs = get_measurements_since(unlikely_date, session=SESSION(),
                                  table=Measurement,
                                  datetime_col="datetime")
    assert logs is None


def test_get_no_days():
    """
    Check a request for the last 0 days returns no results
    """
    logs = get_last_n_days(0, session=SESSION(),
                           table=Measurement, datetime_col="datetime")
    assert logs is None


def test_save_recent_data():
    """test saving data to the test db"""
    recent_data = SAMPLE_JSON
    data_df = process_fetched_data(recent_data, session=SESSION())
    save_recent_data(data_df, table_name="measurements", engine=ENGINE)


def test_some_results():
    """
    Check that the program correctly handles queries that do produce results
    """
    valid_datetime = TEST_TIME - timedelta(1)
    logs = get_measurements_since(valid_datetime, session=SESSION(),
                                  table=Measurement,
                                  datetime_col="datetime")
    assert isinstance(logs, pd.DataFrame)
    assert logs.size


def test_get_last_time():
    """
    Check get_last_time returns a valid datetime
    """
    piid = PI_INFO.loc[0, 'id']
    last_time = get_last_time(piid, session=SESSION())
    assert isinstance(last_time, datetime)


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string
