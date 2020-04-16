echo "# created from data_retrieval.service.sh"
echo "[Unit]"
echo "Description=Retrieve data from pis and add to postgres db"
echo "After=multi-user.target"
echo
echo "[Service]"
echo "WorkingDirectory=$PWD"
echo "User=$USER"
echo "ExecStart=$PWD/env/bin/python3 homesweetpi/retrieve_data.py"
echo
echo "[Install]"
echo "WantedBy=multi-user.target"
