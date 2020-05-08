#!/bin/bash

# check to see if this has been done already
if [ ! -e data_retrieval_service_set_up_complete ]
then
  service_name=data_retrieval
  unit_file=$service_name.service
  timer_file=$service_name.timer
  service_path=/etc/systemd/system/
  # set up logging as a service
  echo "creating $unit_file in $service_path"
  cat > $service_path$unit_file << EOF
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
  echo "Creating $timer_file in $service_path"
  cat > $service_path$timer_file << EOF
[Unit]
Description=Schedule retrieval of data from pis

[Timer]
OnCalendar=*-*-* *:00/5:00
Unit=$unit_file

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
