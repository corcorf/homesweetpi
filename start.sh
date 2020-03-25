#!/bin/bash
export FLASK_APP=homesweetpi/api_server.py
# export FLASK_DEBUG=True
export FLASK_ENV=production
flask run
