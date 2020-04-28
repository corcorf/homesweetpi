"""
Defines SQL tables for the main database in HomeSweetPi as well as functions
for frequently used queries. Tables are defined and handled using the
SQLalchemy ORM.
"""
# pylint: disable=R0903
import os
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import create_engine, distinct
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.inspection import inspect

load_dotenv()

LOG = logging.getLogger("homesweetpi.sql_tables")

HOST = os.getenv('POSTGRES_SERVER_ADDRESS')
PORT = '5432'
USERNAME = 'homesweetpi'
PASSWORD = os.getenv('HSP_PASSWORD')
DB = 'homesweetpi'
BASE = declarative_base()
CONN_STRING = f'postgres://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'
ENGINE = create_engine(CONN_STRING, echo=False)
SESSION = sessionmaker(bind=ENGINE)


class RaspberryPi(BASE):
    """
    Class for Raspberry Pi table in PostGres DB
    _______
    columns:
        id (String)
        name (String)
        ipaddress (String)
    """
    __tablename__ = 'raspberrypis'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    ipaddress = Column(String, nullable=False, unique=True)

    sensors = relationship("Sensor", back_populates="raspberrypi")

    def __repr__(self):
        info = (self.id, self.name, self.ipaddress)
        return "<RaspberryPi(pi={}, sensor={}, pin={})>".format(*info)


class Sensor(BASE):
    """
    Class for Sensors table in PostGres DB
    _______
    columns:
        id (Integer)
        location (String)
        type (String)
        pin (Integer)
        piid (String)
    """
    __tablename__ = 'sensors'
    id = Column(Integer, primary_key=True)
    location = Column(String, nullable=False)
    type = Column(String, nullable=False)
    pin = Column(Integer)
    piid = Column(String, ForeignKey('raspberrypis.id'))

    raspberrypi = relationship('RaspberryPi', back_populates="sensors")
    measurements = relationship('Measurement', back_populates="sensor")

    def __repr__(self):
        info = (self.id, self.location, self.type)
        return "<Sensor(id={}, location={}, type={})>".format(*info)


class Measurement(BASE):
    """
    Class for measurement data table in PostGres DB
    _______
    columns:
        datetime (DateTime)
        sensorid (Integer)
        temp (Float)
        humidity (Float)
        pressure (Float)
        gasvoc (Float)
        mcdvalue (Integer)
        mcdvoltage (Float)
    """
    __tablename__ = 'measurements'

    sensorid = Column(Integer, ForeignKey('sensors.id'), primary_key=True)
    datetime = Column(DateTime, primary_key=True)
    temp = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    gasvoc = Column(Float)
    mcdvalue = Column(Integer)
    mcdvoltage = Column(Float)

    sensor = relationship('Sensor', back_populates="measurements")

    def __repr__(self):
        info = (self.sensorid, self.datetime)
        return "<Measurement(sensor={}, datetime={})>".format(*info)

    def get_row(self):
        """
        Return a dictionary containing the readings for a paricular time and
        sensor, complete with the full sensor information.
        """
        data = dict(
            datetime=self.datetime,
            strftime=self.datetime.strftime("%d.%m.%Y %H:%M:%S"),
            sensorid=self.sensorid,
            sensorlocation=self.sensor.location,
            piname=self.sensor.raspberrypi.name,
            temp=self.temp,
            humidity=self.humidity,
            pressure=self.pressure,
            gasvoc=self.gasvoc,
            mcdvalue=self.mcdvalue,
            mcdvoltage=self.mcdvoltage,
        )
        return data


def create_tables(engine=ENGINE):
    """
    Create all tables in the sql database
    """
    LOG.debug('Creating tables in sql')
    BASE.metadata.create_all(engine)


def get_pi_names(session=SESSION()):
    """
    Return an array of unique raspberry pi names
    """
    LOG.debug("Querying Pi Names")
    query = session.query(RaspberryPi).subquery()
    result = session.query(distinct(query.c.name)).all()
    result = np.array(result).reshape(-1).astype(str)
    result.sort()
    LOG.debug("Pi names: %s", result)
    return result


def get_pi_ips(session=SESSION()):
    """
    Return an array of unique raspberry pi ip addresses
    """
    LOG.debug("Querying Pi ip addresses")
    query = session.query(RaspberryPi).subquery()
    result = session.query(distinct(query.c.ipaddress)).all()
    result = np.array(result).reshape(-1).astype(str)
    result.sort()
    LOG.debug("Pi ips: %s", result)
    return result


def get_pi_names_and_addresses(session=SESSION()):
    """
    Return a list of name, ip-address tuples
    """
    LOG.debug("Querying Pi (name, address) tuples")
    result = session.query(RaspberryPi.name, RaspberryPi.ipaddress).all()
    result.sort()
    LOG.debug("Pi (name, address) tuples: %s", result)
    return result


def get_pi_ids(session=SESSION()):
    """
    Return a list of pi id numbers
    """
    LOG.debug("Querying Pi id numbers")
    result = np.array(session.query(RaspberryPi.id).all()).flatten()
    result.sort()
    LOG.debug("Pi id numbers: %s", result)
    return result


def load_sensor_and_pi_info(pi_file, sensor_file, engine=ENGINE):
    """
    Upload info on pis and sensors to the db
    """
    LOG.debug("Uploading Pi info to db from %s", pi_file)
    pis = pd.read_csv(pi_file)
    pis.to_sql("raspberrypis", engine, index=False, if_exists="append")

    LOG.debug("Uploading sensor info to db from %s", sensor_file)
    sensors = pd.read_csv(sensor_file)
    sensors.drop(['name'], axis=1, inplace=True)
    sensors.to_sql("sensors", engine, index=False, if_exists="append")


def get_sensor_locations(session=SESSION()):
    """
    Return an array of unique sensor locations
    """
    LOG.debug("Querying sensor locations")
    query = session.query(Sensor).subquery()
    result = session.query(distinct(query.c.location)).all()
    result = np.array(result).reshape(-1).astype(str)
    result.sort()
    LOG.debug("Sensor locations: %s", result)
    return result


def get_all_sensors(session=SESSION()):
    """
    Return an array of unique sensor names
    """
    LOG.debug("Querying sensor names")
    query = session.query(Sensor).subquery()
    result = session.query(distinct(query.c.id)).all()
    result = np.array(result).reshape(-1).astype(str)
    result.sort()
    LOG.debug("Sensor names: %s", result)
    return result


def get_sensors_on_pi(piid, session=SESSION()):
    """
    Get a dataframe relating location names and pi name to sensor id for
    a given pi
    """
    LOG.debug("Requesting sensors for pi %s", piid)
    query = session.query(Sensor).join(RaspberryPi)\
                   .filter(RaspberryPi.id == piid)
    sensors = query.values("sensors.id", "location", "name")
    LOG.debug("Result of query for sensors on %s: %s", piid, sensors)
    return pd.DataFrame(sensors).rename(columns={"name": "piname",
                                                 "sensors.id": "sensorid"})


def get_sensors_and_pis(session=SESSION()):
    """
    Return dataframe of sensors with their host pis
    """
    LOG.debug("Querying info for sensors on all pis")
    query = session.query(Sensor).join(RaspberryPi)
    sensors = query.values("sensors.id", "location", "name")
    LOG.debug("Result of query for info on sensors on all pis: %s", sensors)
    return pd.DataFrame(sensors).rename(columns={"name": "piname",
                                                 "sensors.id": "sensorid"})


def get_last_time(piid, session=SESSION()):
    """
    Get the time of the most recent reading for a given raspberry pi
    Returns a datetime
    """
    LOG.debug("Querying time of most recent reading for pi %s", piid)
    query = session.query(Measurement).join(Sensor).join(RaspberryPi)
    query = query.filter(Sensor.piid == piid)\
                 .order_by(Measurement.datetime.desc())
    result = query.first()
    if result is not None:
        last_time = result.datetime
        LOG.debug("Last reading for %s at %s", piid, last_time)
    else:
        last_time = datetime(1970, 1, 1)
        LOG.debug("No readings found for %s, returning %s", piid, last_time)
    return last_time


def get_ip_addr(piid, session=SESSION()):
    """
    Query the SQL database to find the ipaddress for a given pi_id
    Returns a string with the ip address
    """
    LOG.debug("Querying ip address for pi %s", piid)
    query = session.query(RaspberryPi).filter(RaspberryPi.id == piid)
    address = query.first().ipaddress
    LOG.debug("IP address for pi %s i %s", piid, address)
    return address


def one_or_more_results(query):
    """
    Return True if query contains one or more results, otherwise False
    """
    LOG.debug("Checking query returns one or more results")
    try:
        query.one()
    except NoResultFound:
        LOG.debug("Query returns no results")
        return False
    except MultipleResultsFound:
        LOG.debug("Query returns more than one result")
    LOG.debug("Query returns one result")
    return True


def get_measurements_since(since_datetime, session=SESSION(),
                           table=Measurement,
                           datetime_col="datetime"):
    """
    Retrieve all measurements since since_datetime
    Return as a dataframe
    """
    LOG.debug("Querying for all readings since %s", since_datetime)
    query = session.query(table)\
                   .filter(getattr(table, datetime_col) >= since_datetime)
    if one_or_more_results(query):
        output_cols = [c.name for c in inspect(table).columns]
        i = query.values(*output_cols)
        logs = pd.DataFrame(i)
        logs = logs.sort_values(by=datetime_col)
    else:
        logs = None
    return logs


def get_last_n_days(ndays_to_display, session=SESSION(),
                    table=Measurement, datetime_col="datetime"):
    """
    Query the database for measurements from the last n days
    returns a dataframe
    """
    LOG.debug("Querying for all readings in last %s days", ndays_to_display)
    earliest = datetime.now() - timedelta(days=ndays_to_display)
    logs = get_measurements_since(earliest, session,
                                  table, datetime_col)
    return logs


def resample_measurements(logs, resample_freq='30T', datetime_col="datetime",
                          logger_col='sensorid'):
    """
    Resample logs at the given frequency
    return a new dataframe
    """
    LOG.debug("Resampling readings at frequency %s", resample_freq)
    assert isinstance(logger_col, str)
    source = logs.set_index(datetime_col).groupby(logger_col)
    source = source.resample(resample_freq).mean().drop(logger_col, axis=1)
    source = source.reset_index()
    return source


def get_last_measurement_for_sensor(sensorid, session=SESSION()):
    """
    Get the time of the most recent reading for a given sensorid pi
    Returns a dataframe if a measurment is found, else None
    """
    LOG.debug("Querying for last reading from sensor %s", sensorid)
    query = session.query(Measurement).join(Sensor).join(RaspberryPi)\
                   .filter(Sensor.id == sensorid)\
                   .order_by(Measurement.datetime.desc())
    try:
        result = query.first().get_row()
        LOG.debug("Last reading found for sensor %s: %s", sensorid, result)
        return result
    except NoResultFound:
        LOG.debug("No readings found for sensor %s", sensorid)
        return None


def save_recent_data(recent_data, table_name="measurements", engine=ENGINE):
    """
    send a pandas DataFrame of readings pulled from the pi_logger api to SQL
    """
    LOG.debug("Attempting to save data to table %s", table_name)
    LOG.debug("Data to save is %s", recent_data)
    try:
        recent_data.to_sql(table_name, engine, index=False,
                           if_exists="append")
        LOG.debug("No exceptions raised by SQLalchemy on saving data")
        return True
    except sqlalchemy.exc.IntegrityError as exception:
        LOG.debug("sqlalchemy raised IntegrityError: %s", exception)
        return False


if __name__ == "__main__":
    create_tables(ENGINE)
