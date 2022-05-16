#!/usr/bin/env python

import os
import csv
import sys
import glob
import time
import logging
import argparse
import datetime

from influxdb import InfluxDBClient
from dateutil import parser as dt_parse


_base_path = os.getcwd()
logging.basicConfig(filename="python_corsair_icue.log", level=logging.INFO)
logger = logging.getLogger()

path_to_sensor_logs = "/mnt/c/Users/rules/Documents/dev/corsair_logging"


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="File to read")
parser.add_argument("-r", "--read", dest="read", action="store_true", help="Only read file and parse it")
parser.add_argument("-s", "--send", dest="send", action="store_true", help="Send the file contents to the server")
parser.add_argument("-D", "--daemon", dest="daemon", action="store_true", help="Daemonize the reader")


class PyiCue:
    def __init__(self):
        self.list_of_files = glob.glob(f"{path_to_sensor_logs}/*.csv")
        self.influx_host = 'localhost'
        self.influx_port = 8086
        self.influx_user = 'root'
        self.influx_password = 'root'
        self.influx_dbname = 'db0'
        self.influx_client = InfluxDBClient(self.influx_host, self.influx_port, self.influx_user, self.influx_password, self.influx_dbname)


    def main(self):
        #Looping over the files, sending the shit to influx, and deleting the file if already sent
        for sfile in self.list_of_files:
            payload = []
            if args.read:
                logger.info(f"Reading {sfile}")
                logger.info("Processing...")
                file_data = self.read_and_clean_data(sfile)
                for line in file_data:
                    payload.append(self.form_payload_json(line))
                logger.info("Done processing...")
                logger.debug(f"Payload for this file {payload}")
            if args.read and args.send:
                logger.info("Sending points to influx...")
                self.influx_client.write_points(payload, database='db0', batch_size=10000, protocol='json')
                logger.info("Done sending to influx")
                logger.info(f"Deleting {sfile}")
                try:
                    os.remove(sfile)
                except:
                    pass
            logger.info("Done")

    def form_payload_json(self, line):
        timestamp = line.pop('Timestamp')
        logger.debug(timestamp)
        json_body = {
                "measurement": "corsair_icue",
                "tags": {
                    "host": "NOMAD"
                },
                "time": timestamp,
                "fields": line
        }
        logger.debug(json_body)
        return json_body

    def get_last_line_from_latest_file(self):
        latest_file = self.get_latest_file()
        clean_data = self.read_and_clean_data(latest_file)
        sorted_lines = sorted(clean_data, key=lambda d: d['Timestamp'])
        last_line = sorted_lines[-1]
        logger.debug(last_line)
        return last_line

    def read_and_clean_data(self, csv_file):
        data = self.read_csv(csv_file)
        output = []
        for line in data:
            line = self.clean_data(line)
            line = self.clean_timestamp(line)
            output.append(line)
        logger.debug(output)
        return output

    def get_latest_file(self):
        logging.debug(self.list_of_files)
        latest_file = max(self.list_of_files, key=os.path.getctime)
        logging.info(latest_file)
        return latest_file

    def read_csv(self, file):
        data = {}
        with open(file, encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            return list(csv_reader)

    def clean_timestamp(self, data):
        for k in data:     
            if k.strip("\ufeff") == "Timestamp":
                raw_timestamp = data[k]
                dt_timestamp = dt_parse.parse(raw_timestamp.strip("APM"))
                timestamp = dt_timestamp.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                logger.debug(f"{k}, {raw_timestamp}")
                logger.debug(f"{k}, {dt_timestamp}")
                logger.debug(f"{k}, {timestamp}")
        del data['\ufeffTimestamp']
        data['Timestamp'] = timestamp
        logger.debug(f"Timestamp: {data['Timestamp']}")
        return data

    def clean_data(self, data):
        for k in data:
            raw_value = data[k]
            clean_value = raw_value.strip("°CRPM%APMV")
            logger.debug(f"Replacing {k}, {raw_value}, {type(raw_value)}")
            if k != "\ufeffTimestamp":
                data[k] = float(clean_value)
            logger.debug(f"Final value, {k}, {data[k]}")
        return data

    def run_daemon(self):
        while True:
            new_point = self.form_payload_json(self.get_last_line_from_latest_file())
            logger.info(new_point)
            self.influx_client.write_points([new_point], database='db0', batch_size=10000, protocol='json')
            time.sleep(4)
    
 
if __name__ == "__main__":
    args = parser.parse_args()
    py_icue = PyiCue()
    if args.daemon:
        py_icue.run_daemon()
    else:
        py_icue.main()