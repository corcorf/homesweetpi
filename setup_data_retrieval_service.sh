#!/bin/bash

# check to see if this has been done already
flag=data_retrieval_service_set_up_complete
if [ ! -e $flag ]
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
OnCalendar=*-*-* *:01/5:00
Unit=$unit_file

[Install]
WantedBy=multi-user.target
EOF
  echo "reloading systemd daemon and enabling service"
  systemctl daemon-reload
  systemctl start $timer_file
  systemctl enable $timer_file
  echo "creating file '$flag'' as flag"
  touch $flag
fi
