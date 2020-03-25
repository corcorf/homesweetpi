import os
import numpy as np
import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, distinct
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
# from config import HSP_PASSWORD as PASSWORD

HOST = 'localhost'
PORT = '5432'
USERNAME = 'homesweetpi'
PASSWORD = os.getenv('HSP_PASSWORD')
DB = 'homesweetpi'
conn_string = f'postgres://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'
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
    logging.debug(f'Creating tables in sql')
    Base.metadata.create_all(engine)


def get_pi_names(session=Session()):
    """
    Return an array of unique raspberry pi names
    """
    q = session.query(RaspberryPi)
    result = session.query(distinct(q.c.name)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_pi_ips(session=Session()):
    """
    Return an array of unique raspberry pi ip addresses
    """
    q = session.query(RaspberryPi)
    result = session.query(distinct(q.c.ipaddress)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_pi_names_and_addresses(session=Session()):
    """
    Return a list of name, ip-address tuples
    """
    return session.query(RaspberryPi.name, RaspberryPi.ipaddress).all()


def get_pi_ids(session=Session()):
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


def get_all_sensors(session=Session()):
    """
    Return an array of unique sensor names
    """
    q = session.query(Sensor).subquery()
    result = session.query(distinct(q.c.id)).all()
    result = np.array(result).reshape(-1).astype(str)
    return result


def get_sensors_on_pi(piid, session=Session()):
    """
    Get a dataframe relating location names and pi name to sensor id for
    a given pi
    """
    q = session.query(Sensor).join(RaspberryPi).filter(RaspberryPi.id == piid)
    sensors = q.values("sensors.id", "location", "name")
    return pd.DataFrame(sensors).rename(columns={"name": "piname",
                                                 "sensors.id": "sensorid"})


def get_sensors_and_pis(session=Session()):
    """
    Return dataframe of sensors with their host pis
    """
    q = session.query(Sensor).join(RaspberryPi)
    sensors = q.values("sensors.id", "location", "name")
    return pd.DataFrame(sensors).rename(columns={"name": "piname",
                                                 "sensors.id": "sensorid"})


def get_last_time(piid, session=Session()):
    """
    Get the time of the most recent reading for a given raspberry pi
    Returns a datetime
    """
    q = session.query(Measurement).join(Sensor).join(RaspberryPi)
    q = q.filter(Sensor.piid == piid).order_by(Measurement.datetime.desc())
    result = q.first()
    if result is not None:
        return result.datetime
    else:
        return datetime(1970, 1, 1)


def get_ip_addr(piid, session=Session()):
    """
    Query the SQL database to find the ipaddress for a given pi_id
    Returns a string with the ip address
    """
    q = session.query(RaspberryPi).filter(RaspberryPi.id == piid)
    return q.first().ipaddress


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


def get_measurements_since(since_datetime, session=Session(),
                           table=Measurement,
                           datetime_col="datetime",
                           output_cols=["sensorid", "datetime",
                                        "temp", "humidity",
                                        "pressure", "gasvoc"]):
    """
    Retrieve all measurements since since_datetime
    Return as a dataframe
    """
    q = session.query(table)\
               .filter(getattr(table, datetime_col) >= since_datetime)
    if one_or_more_results(q):
        i = q.values(*output_cols)
        logs = pd.DataFrame(i)
    logs = logs.sort_values(by=datetime_col)
    return logs


def get_last_n_days(ndays_to_display=5, session=Session(),
                    table=Measurement,
                    datetime_col="datetime",
                    output_cols=["sensorid", "datetime",
                                 "temp", "humidity",
                                 "pressure", "gasvoc"]):
    """
    Query the database for measurements from the last n days
    returns a dataframe
    """
    earliest = datetime.now() - timedelta(days=ndays_to_display)
    logs = get_measurements_since(earliest, session,
                                  table, datetime_col, output_cols)
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


def get_last_measurement_for_sensor(sensorid, session=Session()):
    """
    Get the time of the most recent reading for a given sensorid pi
    Returns a dataframe if a measurment is found, else None
    """
    q = session.query(Measurement).join(Sensor).join(RaspberryPi)\
               .filter(Sensor.id == sensorid)\
               .order_by(Measurement.datetime.desc())
    try:
        result = q.first().get_row()
        return result
    except NoResultFound:
        return None


if __name__ == "__main__":
    create_tables(ENGINE)
