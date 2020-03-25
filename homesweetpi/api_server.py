"""
API server for the homesweetpi module

adapted from:
https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
"""

import pandas as pd
import logging
import json
from flask import Flask
from flask import render_template, request
from flask_restful import Resource, Api
from sqlalchemy.orm import sessionmaker
from homesweetpi.sql_tables import get_all_sensors, get_last_measurement_for_sensor
from homesweetpi.data_preparation import rewrite_chart, recent_readings_as_html

app = Flask("HOMESWEETPI")
api = Api(app)

app.config.update(
    DEBUG=False,
    SERVER_NAME="0.0.0.0:5002"
)

# Session = sessionmaker(bind=ENGINE)
logging.basicConfig(filename='homesweetpi_api.log', level=logging.DEBUG)

@app.route('/')
def main_page():
    context = dict(
        sub_title="Latest readings:",
        table=recent_readings_as_html()
    )
    return render_template('main_page.html',  **context)


@app.route('/charts')
def charts():
    n_days = 5
    resample_freq = '30T'
    rewrite_chart(n_days, resample_freq)
    context = dict(
        sub_title=f"Readings for the last {n_days} days",

    )
    return render_template('charts.html',  **context)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5002', debug=False)
