"""
API server for the homesweetpi module

adapted from:
https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
"""
# pylint: disable=C0103
import logging
from flask import Flask
from flask import render_template, request
from flask_restful import Resource, Api
from homesweetpi.data_preparation import rewrite_chart,\
                                         recent_readings_as_html,\
                                         get_most_recent_readings

app = Flask("homesweetpi")
api = Api(app)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

LOG = logging.getLogger("homesweetpi.api_server")


class GetLast(Resource):
    """
    API route for getting the most recent sensor data from the DB
    """
    # pylint: disable=R0201
    def get(self):
        """
        Return a JSON with the most recent readings for each sensor in DB
        """
        LOG.info("GetLast triggered")
        return get_most_recent_readings()


def get_n_days_to_display():
    """
    Get the number of days that will be displayed on the chart
    """
    max_days = 100
    default_days = 7
    n_days = request.args.get('n_days', default=default_days, type=int)
    try:
        n_days = int(n_days)
        return min(n_days, max_days)
    except (ValueError, TypeError):
        return default_days


@app.route('/')
def main_page():
    """
    Pass table of latest sensor readings as context for main_page
    """
    LOG.info("Main Page triggered")
    context = dict(
        sub_title="Latest readings:",
        table=recent_readings_as_html()
    )
    return render_template('main_page.html', **context)


@app.route('/charts')
def charts():
    """
    Update Altair chart of sensor readings and pass as context to chart page
    """
    LOG.info("Chart page triggered")
    n_days = get_n_days_to_display()
    resample_freq = '30T'
    rows = [
        "Temperature (°C)", 'Relative Humidity (%)',
        'Pressure (hPa)', 'Gas Resistance (Ω)',
        "Soil Moisture Value", "Soil Moisture (V)"
    ]
    chart_filename = "altair_chart_recent_data.json"
    rewrite_chart(
        rows, n_days, resample_freq,
        filename=f"homesweetpi/static/{chart_filename}"
    )
    context = dict(
        sub_title=f"Readings for the last {n_days} days",
        chart_filename=f"\"static/{chart_filename}\""
    )
    return render_template('charts.html', **context)


@app.route('/air_charts')
def air_charts():
    """
    Update Altair chart of air quality readings and pass as context to chart
    page
    """
    LOG.info("Air chart page triggered")
    n_days = get_n_days_to_display()
    resample_freq = '30T'
    rows = [
        "Temperature (°C)", 'Relative Humidity (%)',
        'Pressure (hPa)', 'Gas Resistance (Ω)',
    ]
    chart_filename = "altair_chart_atmopheric_data.json"
    rewrite_chart(
        rows, n_days, resample_freq,
        filename=f"homesweetpi/static/{chart_filename}"
    )
    context = dict(
        sub_title=f"Readings for the last {n_days} days",
        chart_filename=f"\"static/{chart_filename}\""
    )
    return render_template('charts.html', **context)


@app.route('/plant_charts')
def plant_charts():
    """
    Update Altair chart of soil moisture readings and pass as context to chart
    page
    """
    LOG.info("Plant chart page triggered")
    n_days = get_n_days_to_display()
    resample_freq = '30T'
    rows = [
        "Soil Moisture Value", "Soil Moisture (V)"
    ]
    chart_filename = "altair_chart_soil_moisture_data.json"
    rewrite_chart(
        rows, n_days, resample_freq,
        filename=f"homesweetpi/static/{chart_filename}"
    )
    context = dict(
        sub_title=f"Readings for the last {n_days} days",
        chart_filename=f"\"static/{chart_filename}\""
    )
    return render_template('charts.html', **context)


api.add_resource(GetLast, '/get_last')

if __name__ == '__main__':
    LOG.debug("Running api_server as __main__")
    app.run(host='0.0.0.0', port='5002', debug=False)
