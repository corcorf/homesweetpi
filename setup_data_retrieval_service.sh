#!/bin/bash

# check to see if this has been done already
if [ ! -e data_retrieval_service_set_up_complete ]
then
  # set up logging as a service
  echo "Creating data_retrieval.service in /etc/systemd/system/"
  cat > /etc/systemd/system/data_retrieval.service << EOF
[Unit]
Description=Retrieve data from pis and add to postgres db
After=multi-user.target

[Service]
WorkingDirectory=$PWD
User=$USER
ExecStart=$PWD/env/bin/python3 homesweetpi/retrieve_data.py

[Install]
WantedBy=multi-user.target
EOF
  echo "reloading systemd daemon and enabling service"
  systemctl daemon-reload
  systemctl start data_retrieval.service
  systemctl enable data_retrieval.service
  echo "creating file 'data_retrieval_service_set_up_complete as flag"
  touch data_retrieval_service_set_up_complete
fi
