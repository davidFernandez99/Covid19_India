""""
This file will automatically insert all json bodies of each month.
"""
from influxdb import InfluxDBClient
from os import listdir
import os
import json
import pandas as pd


# Get path of the Json directory
dir_path = os.path.dirname(os.path.realpath(__file__)) + '/data'
print(dir_path)


json_files = [dir_path + f'/{f}' for f in listdir(dir_path)]
print(json_files)

with open(json_files[-1]) as json_f:
    # insert_default_json(client, json.load(json_f))
    print("done")
    print(json.load(json_f))

# Create new client to connect with InfluxDB
client = InfluxDBClient(host="localhost", port=8086)

"""
# Creating a new database
client.create_database("weather_db")
print("Database created!")

# Show databases
print(client.get_list_database())


# Select database
client.switch_database("weather_db")

for f in json_files:
    with open(f) as json_f:
        # insert_default_json(client, json.load(json_f))
        print("done")
        print(pd.DataFrame(json.load(json_f)).head(1))
"""