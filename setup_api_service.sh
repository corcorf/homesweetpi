#! /bin/bash

# check to see if this has been done already
if [ ! -e api_service_set_up_complete ]
then
  # set up logging as a service
  echo "copying homesweetpi.service to  /etc/systemd/system/"
  cp homesweetpi.service /etc/systemd/system/homesweetpi.service
  echo "reloading systemd daemon and enabling service"
  systemctl daemon-reload
  systemctl start homesweetpi.service
  systemctl enable homesweetpi.service
  echo "creating file 'api_service_set_up_complete as flag"
  touch api_service_set_up_complete
fi
