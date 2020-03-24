import os
import numpy as np
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, distinct
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

HOST = 'localhost'
PORT = '5432'
USERNAME = 'homesweetpi'
PASSWORD = os.getenv('HSP_PASSWORD')
DB = 'homesweetpi'
conn_string = f'postgres://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'
# CONN_STRING = 'sqlite:///{}'.format(DB_PATH)
Base = declarative_base()
ENGINE = create_engine(conn_string, echo=False)
Session = sessionmaker(bind=ENGINE)


class RaspberryPi(Base):
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


class Sensor(Base):
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


class Measurement(Base):
    """
    Class for measurement data table in PostGres DB
    _______
    columns:
        id (Integer)
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
        data = dict(
            sensorid=self.sensorid,
            datetime=self.datetime,
            temp=self.temp,
            humidity=self.humidity,
            pressure=self.pressure,
            gasvoc=self.gasvoc,
        )
        return data


def create_tables(engine):
    """
    Creates all tables in the sql database
    """
    logging.debug(f'Creating tables in sql')
    Base.metadata.create_all(engine)


def get_pi_names():
    """
    Return an array of unique raspberry pi names
    """
    session = Session()
    q = session.query(RaspberryPi)
    result = session.query(distinct(q.c.name)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_pi_ips():
    """
    Return an array of unique raspberry pi ip addresses
    """
    session = Session()
    q = session.query(RaspberryPi)
    result = session.query(distinct(q.c.ipaddress)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_pi_names_and_addresses():
    """Return a list of name, ip-address tuples"""
    session = Session()
    return session.query(RaspberryPi.name, RaspberryPi.ipaddress).all()


def get_pi_ids():
    """Return a list of pi id numbers"""
    session = Session()
    return np.array(session.query(RaspberryPi.id).all()).flatten()


def load_sensor_and_pi_info(pi_file, sensor_file):
    """
    Upload info on pis and sensors to the db
    """
    pis = pd.read_csv(pi_file)
    pis.to_sql("raspberrypis", ENGINE, index=False, if_exists="append")

    sensors = pd.read_csv(sensor_file)
    sensors.drop(['name'], axis=1, inplace=True)
    sensors.to_sql("sensors", ENGINE, index=False, if_exists="append")


def get_all_sensors():
    """
    Return an array of unique raspberry pi names
    """
    session = Session()
    q = session.query(Sensor)
    result = session.query(distinct(q.c.name)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_sensors_on_pi(piid):
    """
    Get a dataframe relating location names and pi name to sensor id for
    a given pi
    """
    session = Session()
    q = session.query(Sensor).join(RaspberryPi).filter(RaspberryPi.id == piid)
    sensors = q.values("sensors.id", "location", "name")
    return pd.DataFrame(sensors).rename(columns={"name": "piname",
                                                 "sensors.id": "sensorid"})


def get_last_time(piid):
    """
    Get the time of the most recent reading for a given raspberry pi
    Returns a datetime
    """
    session = Session()
    q = session.query(Measurement).join(Sensor).join(RaspberryPi)
    q = q.filter(Sensor.piid == piid).order_by(Measurement.datetime.desc())
    result = q.first()
    if result is not None:
        return result.datetime
    else:
        return datetime(1970, 1, 1)


def get_ip_addr(piid):
    """
    Query the SQL database to find the ipaddress for a given pi_id
    Returns a string with the ip address
    """
    session = Session()
    q = session.query(RaspberryPi).filter(RaspberryPi.id == piid)
    return q.first().ipaddress


if __name__ == "__main__":
    create_tables(ENGINE)
