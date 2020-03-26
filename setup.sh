#! /bin/bash

# check to see if this has been done already
if [ ! -e data_retrieval_service_set_up_complete ]
then
  # set up logging as a service
  echo "copying logger.service to  /etc/systemd/system/"
  cp data_retrieval.service /etc/systemd/system/data_retrieval.service
  echo "reloading systemd daemon and enabling service"
  systemctl daemon-reload
  systemctl start data_retrieval.service
  systemctl enable data_retrieval.service
  echo "creating file 'data_retrieval_service_set_up_complete as flag"
  touch data_retrieval_service_set_up_complete
fi

if [ ! -e env ]
then
  python3 -m venv env
  source env/bin/activate
  pip3 install -r requirements.txt
fi
