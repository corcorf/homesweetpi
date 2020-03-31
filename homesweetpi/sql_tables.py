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
from sqlalchemy import create_engine, distinct
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.inspection import inspect

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
    """
    __tablename__ = 'measurements'

    sensorid = Column(Integer, ForeignKey('sensors.id'), primary_key=True)
    datetime = Column(DateTime, primary_key=True)
    temp = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    gasvoc = Column(Float)

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
        )
        return data


def create_tables(engine=ENGINE):
    """
    Create all tables in the sql database
    """
    logging.debug('Creating tables in sql')
    BASE.metadata.create_all(engine)


def get_pi_names(session=SESSION()):
    """
    Return an array of unique raspberry pi names
    """
    query = session.query(RaspberryPi)
    result = session.query(distinct(query.c.name)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_pi_ips(session=SESSION()):
    """
    Return an array of unique raspberry pi ip addresses
    """
    query = session.query(RaspberryPi)
    result = session.query(distinct(query.c.ipaddress)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_pi_names_and_addresses(session=SESSION()):
    """
    Return a list of name, ip-address tuples
    """
    return session.query(RaspberryPi.name, RaspberryPi.ipaddress).all()


def get_pi_ids(session=SESSION()):
    """
    Return a list of pi id numbers
    """
    return np.array(session.query(RaspberryPi.id).all()).flatten()


def load_sensor_and_pi_info(pi_file, sensor_file, engine=ENGINE):
    """
    Upload info on pis and sensors to the db
    """
    pis = pd.read_csv(pi_file)
    pis.to_sql("raspberrypis", engine, index=False, if_exists="append")

    sensors = pd.read_csv(sensor_file)
    sensors.drop(['name'], axis=1, inplace=True)
    sensors.to_sql("sensors", engine, index=False, if_exists="append")


def get_all_sensors(session=SESSION()):
    """
    Return an array of unique sensor names
    """
    query = session.query(Sensor).subquery()
    result = session.query(distinct(query.c.id)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_sensors_on_pi(piid, session=SESSION()):
    """
    Get a dataframe relating location names and pi name to sensor id for
    a given pi
    """
    query = session.query(Sensor).join(RaspberryPi)\
                   .filter(RaspberryPi.id == piid)
    sensors = query.values("sensors.id", "location", "name")
    return pd.DataFrame(sensors).rename(columns={"name": "piname",
                                                 "sensors.id": "sensorid"})


def get_sensors_and_pis(session=SESSION()):
    """
    Return dataframe of sensors with their host pis
    """
    query = session.query(Sensor).join(RaspberryPi)
    sensors = query.values("sensors.id", "location", "name")
    return pd.DataFrame(sensors).rename(columns={"name": "piname",
                                                 "sensors.id": "sensorid"})


def get_last_time(piid, session=SESSION()):
    """
    Get the time of the most recent reading for a given raspberry pi
    Returns a datetime
    """
    query = session.query(Measurement).join(Sensor).join(RaspberryPi)
    query = query.filter(Sensor.piid == piid)\
                 .order_by(Measurement.datetime.desc())
    result = query.first()
    if result is not None:
        last_time = result.datetime
    else:
        last_time = datetime(1970, 1, 1)
    return last_time


def get_ip_addr(piid, session=SESSION()):
    """
    Query the SQL database to find the ipaddress for a given pi_id
    Returns a string with the ip address
    """
    query = session.query(RaspberryPi).filter(RaspberryPi.id == piid)
    return query.first().ipaddress


def one_or_more_results(query):
    """
    Return True if query contains one or more results, otherwise False
    """
    try:
        query.one()
    except NoResultFound:
        return False
    except MultipleResultsFound:
        pass
    return True


def get_measurements_since(since_datetime, session=SESSION(),
                           table=Measurement,
                           datetime_col="datetime"):
    """
    Retrieve all measurements since since_datetime
    Return as a dataframe
    """
    query = session.query(table)\
                   .filter(getattr(table, datetime_col) >= since_datetime)
    if one_or_more_results(query):
        output_cols = [c.name for c in inspect(table).columns]
        i = query.values(*output_cols)
        logs = pd.DataFrame(i)
    logs = logs.sort_values(by=datetime_col)
    return logs


def get_last_n_days(ndays_to_display, session=SESSION(),
                    table=Measurement, datetime_col="datetime"):
    """
    Query the database for measurements from the last n days
    returns a dataframe
    """
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
    query = session.query(Measurement).join(Sensor).join(RaspberryPi)\
                   .filter(Sensor.id == sensorid)\
                   .order_by(Measurement.datetime.desc())
    try:
        result = query.first().get_row()
        return result
    except NoResultFound:
        return None


if __name__ == "__main__":
    create_tables(ENGINE)
