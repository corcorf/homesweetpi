echo "[Unit]"
echo "Description=Run homesweetpi api server"
echo "After=multi-user.target"
echo
echo "[Service]"
echo "WorkingDirectory=$PWD"
echo "User=$USER"
echo "ExecStart=/usr/bin/bash start.sh"
echo
echo "[Install]"
echo "WantedBy=multi-user.target"
