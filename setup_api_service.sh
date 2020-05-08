#! /bin/bash

# check to see if this has been done already
if [ ! -e api_service_set_up_complete ]
then
  # set up logging as a service
  echo "Creating homesweetpi.service in /etc/systemd/system/"
  cat > /etc/systemd/system/homesweetpi.service << EOF
[Unit]
Description=Run homesweetpi api server
After=multi-user.target

[Service]
WorkingDirectory=$PWD
User=$USER
ExecStart=/usr/bin/bash start.sh

[Install]
WantedBy=multi-user.target
EOF
  echo "reloading systemd daemon and enabling service"
  systemctl daemon-reload
  systemctl start homesweetpi.service
  systemctl enable homesweetpi.service
  echo "creating file 'api_service_set_up_complete as flag"
  touch api_service_set_up_complete
fi
