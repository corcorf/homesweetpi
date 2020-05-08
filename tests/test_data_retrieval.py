#!/usr/bin/env python

"""
Tests for homesweetpi's data_retrieval module.
"""

import os
import pytest
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from homesweetpi.retrieve_data import process_fetched_data, fetch_recent_data

LOG = logging.getLogger("homesweetpi.test_sql_tables")
TEST_DB_PATH = os.getcwd()
TEST_DB_FILENAME = "test_connection.db"
TEST_DB_FILEPATH = os.path.join(TEST_DB_PATH, TEST_DB_FILENAME)
CONN_STRING = f'sqlite:///{TEST_DB_FILEPATH}'
ENGINE = create_engine(CONN_STRING, echo=False)
SESSION = sessionmaker(bind=ENGINE)


def test_connection_handling():
    """
    Check the programs ability to handle connection problems gracefully
    test_connection.db contains raspberrypi with piip '0000abcd' and an
    invalid ip address to test what happens when the program cannot reach
    the api
    """
    piid = '0000abcd'
    qtime = datetime.now().time()
    fetch_recent_data(pi_id=piid, query_time=qtime, session=SESSION(),
                      port=9999)
