#! /bin/bash

# check to see if this has been done already
flag=api_service_set_up_complete
if [ ! -e $flag ]
then
  service_name=homesweetpi
  unit_file=$service_name.service
  service_path=/etc/systemd/system/
  # set up logging as a service
  echo "creating $unit_file in $service_path"
  cat > $service_path$unit_file << EOF
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
  systemctl start $unit_file
  systemctl enable $unit_file
  echo "creating file '$flag' as flag"
  touch $flag
fi
