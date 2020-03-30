"""
API server for the homesweetpi module

adapted from:
https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
"""

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

# Session = sessionmaker(bind=ENGINE)
logging.basicConfig(filename='homesweetpi_api.log', level=logging.DEBUG)


class GetLast(Resource):
    """
    API route for getting the most recent sensor data from the DB
    """
    def get(self):
        """
        Return a JSON with the most recent readings for each sensor in DB
        """
        return get_most_recent_readings()


@app.route('/')
def main_page():
    """
    Pass table of latest sensor readings as context for main_page
    """
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
    max_days = 100
    default_days = 7
    n_days = request.args.get('n_days', default=default_days, type=int)
    try:
        n_days = int(n_days)
        n_days = min(n_days, max_days)
    except (ValueError, TypeError):
        n_days = default_days
    resample_freq = '30T'
    rewrite_chart(n_days, resample_freq)
    context = dict(
        sub_title=f"Readings for the last {n_days} days",

    )
    return render_template('charts.html', **context)


api.add_resource(GetLast, '/get_last')

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port='5002', debug=False)
