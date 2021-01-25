#!/bin/sh
# /etc/systemd/system/deluge.service

[Unit]
Description=Deluge daemon
After=Network.target

[Service]
Type=simple
ExecStart=/usr/bin/bash /usr/local/bin/deluge.sh
ExecStop=/usr/bin/bash /usr/local/bin/deluge.sh
ExecReload=/usr/bin/bash /usr/local/bin/deluge.sh

[Install]
WantedBy=multi-user.target
