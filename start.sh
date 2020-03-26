#!/bin/bash
#export FLASK_APP=homesweetpi/api_server.py
#export FLASK_ENV=production
#export FLASK_DEBUG=True
#export FLASK_RUN_PORT=5002
source /home/ubuntu/homesweetpi/env/bin/activate
flask run --host=0.0.0.0
