#!/bin/bash
export FLASK_APP=homesweetpi/api_server.py
export FLASK_ENV=production
export FLASK_DEBUG=False
# export FLASK_SERVER_NAME="0.0.0.0:5002"
flask run
