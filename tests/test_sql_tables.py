#!/usr/bin/env python

"""
Tests for homesweetpi's sql_tables module.
Creates an SQLite db instead of the usual PostGres
"""

import os
import pytest
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from homesweetpi.sql_tables import create_tables, load_sensor_and_pi_info,\
                                   get_pi_names, get_sensor_locations,\
                                   get_last_time, save_recent_data,\
                                   one_or_more_results, Measurement,\
                                   get_measurements_since, get_last_n_days
from homesweetpi.retrieve_data import process_fetched_data

LOG = logging.getLogger("homesweetpi.test_sql_tables")

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

SAMPLE_JSON = '{"id": {"0": 101, "1": 102}, \
"datetime": {"0": %s, "1": %s}, \
"location": {"0": "bay", "1": "allo"}, \
"sensortype": {"0": "MCP", "1": "MCP"}, \
"piname": {"0": "catflap", "1": "catflap"}, \
"piid": {"0": "100000003d12f229", "1": "100000003d12f229"}, \
"temp": {"0": null, "1": null}, \
"humidity": {"0": null, "1": null}, \
"pressure": {"0": null, "1": null}, \
"gasvoc": {"0": null, "1": null}, \
"mcdvalue": {"0": 64576, "1": 22976}, \
"mcdvoltage": {"0": 3.2517097734, "1": 1.1601739528}}'\
% (round(TEST_TIME.timestamp()*1000), round(TEST_TIME.timestamp()*1000))


def test_create_db():
    """Test creating the tables in SQLite DB"""
    create_tables(ENGINE)
    assert os.path.exists(TEST_DB_FILEPATH)


def test_db_connection_failure():
    """Check an error is raised if connection to db fails"""
    host = 'localhost'  # invalid ip address
    port = '5432'
    username = 'homesweetpi'
    password = 'this_is_not_the_password'
    db = 'homesweetpi'
    conn_string = f'postgresql://{username}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_string, echo=False)
    pytest.raises(OperationalError, create_tables, engine)


def test_load_sensor_and_pi_info():
    """test loading sensor an pi info to test.db"""
    load_sensor_and_pi_info(PI_FILE, SENSOR_FILE, engine=ENGINE)


def test_get_pi_names_returns_array():
    """test that get_pi_names returns a numpy array"""
    pi_names_from_db = get_pi_names(session=SESSION())
    assert isinstance(pi_names_from_db, np.ndarray)


def test_get_pi_names_returns_expected():
    """test get_pi_names returns expected result"""
    pi_names_from_db = get_pi_names(session=SESSION())
    pi_names_from_db.sort()
    pi_names_from_file = PI_INFO['name'].unique()
    pi_names_from_file.sort()
    LOG.debug("Pi names from file: %s", pi_names_from_file)
    assert np.all(pi_names_from_db == pi_names_from_file)


def test_get_sensor_locations_returns_array():
    """test that get_sensor_locations returns a numpy array"""
    sensor_locations_from_db = get_sensor_locations(session=SESSION())
    assert isinstance(sensor_locations_from_db, np.ndarray)


def test_get_sensor_locations_returns_expected():
    """add sensor info from file to test.db"""
    sensor_locations_from_db = get_sensor_locations(session=SESSION())
    sensor_locations_from_file = SENSOR_CONFIG['location'].unique()
    sensor_locations_from_file.sort()
    LOG.debug("Sensor location from file: %s", sensor_locations_from_file)
    assert np.all(sensor_locations_from_db == sensor_locations_from_file)


def test_process_fetched_data_returns_df():
    """
    check that sample json can be processed and transformed into a dataframe
    """
    recent_data = SAMPLE_JSON
    data_df = process_fetched_data(recent_data, session=SESSION())
    assert isinstance(data_df, pd.DataFrame)


def test_processed_data_contains_datetime_col():
    """
    check that sample json can be processed and transformed into a dataframe
    """
    recent_data = SAMPLE_JSON
    data_df = process_fetched_data(recent_data, session=SESSION())
    assert 'datetime' in data_df.columns


def test_no_results_from_future_timestamp():
    """
    Check that the program correctly handles queries that produce no results
    """
    unlikely_date = datetime(2099, 1, 1, 0, 0)
    session = SESSION()
    query = session.query(Measurement)\
                   .filter(getattr(Measurement, "datetime") >= unlikely_date)
    assert not one_or_more_results(query)


def test_get_measurements_since_with_future_timestamp():
    """
    Check that the program correctly handles queries that produce no results
    """
    unlikely_date = datetime(2099, 1, 1, 0, 0)
    session = SESSION()
    logs = get_measurements_since(unlikely_date, session=session,
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
    assert save_recent_data(data_df, table_name="measurements", engine=ENGINE)


def test_get_last_n_days_returns_df():
    """
    Check that the program correctly handles queries that do produce results
    """
    logs = get_last_n_days(1, session=SESSION(),
                           table=Measurement, datetime_col="datetime")
    assert isinstance(logs, pd.DataFrame)


def test_some_results_1_day():
    """
    Check that the program correctly handles queries that do produce results
    """
    logs = get_last_n_days(1, session=SESSION(),
                           table=Measurement, datetime_col="datetime")
    assert logs.size


def test_get_measurements_since_returns_df():
    """
    Check that the program correctly handles queries that do produce results
    """
    valid_datetime = TEST_TIME - timedelta(1)
    logs = get_measurements_since(valid_datetime, session=SESSION(),
                                  table=Measurement,
                                  datetime_col="datetime")
    assert isinstance(logs, pd.DataFrame)


def test_some_results():
    """
    Check that the program correctly handles queries that do produce results
    """
    valid_datetime = TEST_TIME - timedelta(1)
    logs = get_measurements_since(valid_datetime, session=SESSION(),
                                  table=Measurement,
                                  datetime_col="datetime")
    assert logs.size


def test_get_last_time():
    """
    Check get_last_time returns a valid datetime
    """
    piid = PI_INFO.loc[0, 'id']
    last_time = get_last_time(piid, session=SESSION())
    assert isinstance(last_time, datetime)
