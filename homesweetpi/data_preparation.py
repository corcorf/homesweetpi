import pandas as pd
import altair as alt
import logging
from sqlalchemy import distinct
from homesweetpi.sql_tables import Measurement, Sensor, RaspberryPi, Base,\
                                   get_sensors_on_pi, one_or_more_results,\
                                   get_last_time, get_ip_addr,\
                                   get_last_n_days, resample_measurements,\
                                   get_last_measurement_for_sensor,\
                                   get_all_sensors, get_sensors_and_pis
from datetime import datetime, timedelta
import json


def create_selection(datetime_col="Time"):
    """
    Component of Altair plot creation.
    Create a selection object for an Altair plot that chooses the nearest
    point & selects based on x-value
    """
    return alt.selection(type='single', on='mouseover',
                              fields=[datetime_col], nearest=True)


def create_lines(source, datetime_col="Time", logger_col="Location"):
    """
    Component of Altair plot creation.
    create lines object for an Altair plot
    """
    lines = alt.Chart(source).mark_line(
    ).encode(
        alt.X(datetime_col, type='temporal'),
        alt.Y(alt.repeat("row"), type='quantitative',
              scale=alt.Scale(zero=False)),
        color=f'{logger_col}:N',
    )
    return lines


def create_selectors(source, nearest, datetime_col="datetime"):
    """
    Component of Altair plot creation.
    Transparent selectors across the chart. This is what tells us
    the x-value of the cursor
    """
    selectors = alt.Chart(source).mark_point().encode(
        x=f'{datetime_col}:T',
        opacity=alt.value(0),
    ).add_selection(
        nearest
    )
    return selectors


def create_points(lines, nearest):
    """
    Component of Altair plot creation.
    Draw points on the line, and highlight based on selection
    """
    points = lines.mark_point().encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )
    return points


def create_text(lines, nearest):
    """
    Component of Altair plot creation.
    Draw text labels near the points, and highlight based on selection
    """
    text = lines.mark_text(align='left', dx=5, dy=-5).encode(
        text=alt.condition(nearest, alt.Y(alt.repeat("row"),
                                          type='quantitative'),
                           alt.value(' '))
    )
    return text


def create_rules(source, nearest, datetime_col="datetime"):
    """
    Component of Altair plot creation.
    Draw a rule at the location of the selection
    """
    rules = alt.Chart(source).mark_rule(color='gray').encode(
        x=f'{datetime_col}:T',
    ).transform_filter(
        nearest
    )
    return rules


def create_chart(lines, selectors, points, rules, text,
                 row_repeat=['Temperature', 'Relative Humidity',
                             'Pressure', 'Air Quality'],
                 width=600, height=200):
    """
    Component of Altair plot creation.
    Put the five layers into a chart and bind the data
    """
    chart = alt.layer(lines, selectors, points, rules, text
                      ).properties(
                          width=width, height=height
                      ).repeat(row=row_repeat).interactive()
    return chart


def create_altair_plot(source, datetime_col='Time', logger_col='Location'):
    """
    Return an interactive altair chart object from the source data
    """
    nearest = create_selection()
    lines = create_lines(source, datetime_col, logger_col)
    selectors = create_selectors(source, nearest, datetime_col)
    points = create_points(lines, nearest)
    text = create_text(lines, nearest)
    rules = create_rules(source, nearest, datetime_col)
    chart = create_chart(lines, selectors, points, rules, text,
                         ['Temperature', 'Relative Humidity',
                          'Pressure', 'Air Quality'],
                         600, 200)
    return chart


def rewrite_chart(n_days=5, resample_freq='30T',
                  fn="homesweetpi/static/altair_chart_recent_data.json"):
    """
    create an altair chart with data from the last n days and save as json
    """
    logs = get_last_n_days(n_days)
    source = resample_measurements(logs, resample_freq).round(1)
    lookup = get_sensors_and_pis().set_index("sensorid")['location']
    source['sensorid'] = source['sensorid'].apply(lookup.get)
    source = source.rename(columns={
        "datetime": "Time",
        "temp": "Temperature",
        "humidity": "Relative Humidity",
        "pressure": "Pressure",
        "gasvoc": "Air Quality",
        "sensorid": "Location"
    })

    chart = create_altair_plot(source)
    chart.save(fn)


def get_most_recent_readings():
    """
    Return a json containing the most recent readings for all sensors
    """
    recent_readings = {}
    for sensor in get_all_sensors():
        measurement = get_last_measurement_for_sensor(sensor)
        measurement.pop("datetime")
        recent_readings[sensor] = measurement
    return json.dumps(recent_readings)


def recent_readings_as_html():
    """
    Convert json of latest sensor results to html for rending
    """
    df = pd.read_json(get_most_recent_readings()).T
    df = df.rename(columns={
        "strftime": "Time", "sensorid": "Sensor ID",
        "sensorlocation": "Location",
        "temp": "Temperature", "humidity": "Humidity", "pressure": "Pressure",
        "gasvoc": "Air Quality", "piname": "Pi",
    })
    df['Time'] = pd.to_datetime(df['Time'])
    df = df.astype({"Temperature": float, "Humidity": float, "Pressure": float,
                    "Air Quality": float})
    df = df.round(1)
    cols = ["Time", "Location", "Temperature", "Humidity", "Pressure",
            "Air Quality"]
    table = df.to_html(columns=cols, index=False, justify='left',
                       classes="table", table_id="latest_results")
    return table
