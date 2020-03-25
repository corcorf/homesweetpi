import pandas as pd
import altair as alt
import logging
from sqlalchemy import distinct
from homesweetpi.sql_tables import Measurement, Sensor, RaspberryPi, Base
from homesweetpi.sql_tables import get_last_time, get_ip_addr,\
                                   get_sensors_on_pi, one_or_more_results
from datetime import datetime, timedelta
import json


def create_selection():
    """
    Component of Altair plot creation.
    Create a selection object for an Altair plot that chooses the nearest
    point & selects based on x-value
    """
    return alt.selection(type='single', on='mouseover',
                              fields=['datetime'], nearest=True)


def create_lines(source, datetime_col="datetime", logger_col="sensorid"):
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
                 row_repeat=['temp', 'humidity',
                             'pressure', 'gasvoc'],
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


def create_altair_plot(source, datetime_col='datetime', logger_col='sensorid'):
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
                         ['temp', 'humidity', 'pressure', 'gasvoc'],
                         600, 200)
    return chart
