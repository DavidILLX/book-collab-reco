#!/bin/bash
set -e 

apt-get clean
apt-get update
apt-get install -y python3 python3-pip

pip3 install --no-cache-dir -r /docker-entrypoint-initdb.d/requirements.txt