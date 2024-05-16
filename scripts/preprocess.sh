#!/bin/bash

echo "...installation"
if [ ! -f "get-pip.py" ]; then
	wget https://bootstrap.pypa.io/pip/2.7/get-pip.py
	python get-pip.py
fi

echo "pip installed"

pip install -r ../requirements.txt --ignore-installed

if [ ! -f "postgresql-42.6.0.jar" ]; then
	wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar --no-check-certificate
	cp  postgresql-42.6.0.jar /usr/hdp/current/sqoop-client/lib/
fi

rm -f /var/lib/pgsql/data/pg_hba.conf
cp ./config/pg_hba.conf /var/lib/pgsql/data/pg_hba.conf
sudo systemctl restart postgresql
echo "Configured postgresql"

if [ ! -f "./data/data.csv" ]; then
	wget -O ./data/data.csv https://github.com/sashaismonster/big-data-project-2024/blob/main/data/data.csv
fi

echo "data is loaded, next is preprocessing"

python ./scripts/preprocess.py -i "./data/data.csv" -o "./data"
echo "data is processed"
