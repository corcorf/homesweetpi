#!/bin/bash

if [ ! -e env ]
then
  python3 -m venv env
  source env/bin/activate
  pip3 install -e .
fi
