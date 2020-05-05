"""
Module to retrieve data from the PostGresDB at a specified frequency
"""
import json
import logging
import pandas as pd
import altair as alt
from homesweetpi.sql_tables import (get_last_n_days, resample_measurements,
                                    get_last_measurement_for_sensor,
                                    get_all_sensors, get_sensors_and_pis,
                                    Measurement)

LOG = logging.getLogger("homesweetpi.data_preparation")


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


def create_chart(chart_components,
                 rows,
                 width=600, height=200):
    """
    Component of Altair plot creation.
    Put the components into a chart and bind the data
    chart_components is a dictionary containing Altair chart components,
    eg lines, selectors, points, rules, text,
    """
    LOG.debug("Creating Altair Chart object")
    chart = alt.layer(*chart_components.values(),
                      ).properties(
                          width=width, height=height
                      ).repeat(row=rows).interactive()
    return chart


def format_chart(chart):
    """
    Customise chart formatting
    return formatted chart instance
    """
    LOG.debug("Formatting Altair Chart object")
    chart = chart.configure_title(fontSize=16,
                                  color='darkgray',
                                  anchor="start",
                                  frame="group")
    chart = chart.configure_axisLeft(labelFontSize=12,
                                     titleFontSize=15)
    chart = chart.configure_axisBottom(labelFontSize=12,
                                       title=None)
    chart = chart.configure_legend(labelFontSize=12,
                                   titleFontSize=12)
    return chart


def create_altair_plot(source, rows, datetime_col='Time',
                       logger_col='Location', title="HomeSweetPi Data"):
    """
    Return an interactive altair chart object from the source data
    """
    LOG.debug("Creating Altair Chart components")
    nearest = create_selection()
    lines = create_lines(source, datetime_col, logger_col)
    chart_components = dict(
        lines=lines,
        selectors=create_selectors(source, nearest, datetime_col),
        points=create_points(lines, nearest),
        text=create_text(lines, nearest),
        rules=create_rules(source, nearest, datetime_col),
    )
    chart = create_chart(chart_components, rows, 600, 200
                         ).properties(title=title)
    chart = format_chart(chart)
    return chart


def prepare_chart_data(logs, resample_freq='30T'):
    """
    Prepare the data for creation of the Altair plot
    """
    LOG.debug("Preparing data for Altair Chart")
    source = resample_measurements(logs, resample_freq).round(1)
    lookup = get_sensors_and_pis().set_index("sensorid")['location']
    source['sensorid'] = source['sensorid'].apply(lookup.get)
    source = source.rename(Measurement().get_fancy_names_dict())
    return source


def rewrite_chart(rows, n_days=5, resample_freq='30T',
                  filename="homesweetpi/static/altair_chart_recent_data.json"):
    """
    create an altair chart with data from the last n days and save as json
    """
    LOG.debug("Rewriting Altair Chart object")
    title = f"Readings from the last {n_days} days:"
    logs = get_last_n_days(n_days)
    source = prepare_chart_data(logs, resample_freq)
    chart = create_altair_plot(source, rows, title=title)
    chart.save(filename)


def get_most_recent_readings(current_only=False):
    """
    Return a json containing the most recent readings for all sensors
    """
    LOG.debug("Requesting most recent readings")
    recent_readings = {}
    for sensor in get_all_sensors(current_only=current_only):
        measurement = get_last_measurement_for_sensor(sensor)
        measurement.pop("datetime")
        recent_readings[sensor] = measurement
    return json.dumps(recent_readings)


def recent_readings_as_html(current_only=True):
    """
    Convert json of latest sensor results to html for rending
    """
    readings = pd.read_json(
        get_most_recent_readings(current_only=current_only)
    ).T
    LOG.debug("Converting most recent readings to html")
    readings = readings.rename(columns={
        "strftime": "Time", "sensorid": "Sensor ID",
        "sensorlocation": "Location",
        "temp": "Temperature (°C)", "humidity": "Humidity (%)",
        "pressure": "Pressure (hPa)", "gasvoc": "Gas Resistance (Ω)",
        "piname": "Pi", "mcdvalue": "Soil Moisture Value",
        "mcdvoltage": "Soil Moisture (V)"
    })
    readings['Time'] = pd.to_datetime(readings['Time'])
    readings = readings.astype({
        "Temperature (°C)": float, "Humidity (%)": float,
        "Pressure (hPa)": float, "Gas Resistance (Ω)": float,
        "Soil Moisture Value": float, "Soil Moisture (V)": float,
    })
    readings = readings.round(1)
    cols = [
        "Time", "Location", "Temperature (°C)", "Humidity (%)",
        "Pressure (hPa)", "Gas Resistance (Ω)", "Soil Moisture (V)"
    ]
    table = readings.to_html(
        columns=cols, index=False, justify='left', na_rep='-',
        classes="table", table_id="latest_results"
    )
    return table
