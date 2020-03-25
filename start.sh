#!/bin/bash
export FLASK_APP=homesweetpi/api_server.py
export FLASK_ENV=production
export FLASK_DEBUG=False
flask run
