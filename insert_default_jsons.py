""""
This file will automatically insert all json bodies of each month.
"""
from influxdb import InfluxDBClient
from os import listdir
import os
import json
from resources import insert_default_json, convert_to_milliseconds
import datetime

# Get path of the Json directory
dir_path = os.path.dirname(os.path.realpath(__file__)) + '/data'

json_files = [dir_path + f'/{f}' for f in listdir(dir_path)]

# Create new client to connect with InfluxDB
try:
    client = InfluxDBClient(host="localhost", port=8086)
except Exception as e:
    print(e)

try:
    # Creating a new database
    client.create_database("weather_db")
    print("Database created!")
except Exception as e:
    print(str(e))
    pass
# Show databases
print(client.get_list_database())


# Select database
client.switch_database("weather_db")
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November",
          "December"]

for month in months:
    client.drop_measurement(month)
print("Result: {0}".format(client.get_list_measurements()))
for f in json_files:
    with open(f) as json_f:
        list_data = json.load(json_f)["cases_time_series"]
        for day in  list_data:
            measurement = day["date"].split()[1]
            time = convert_to_milliseconds(datetime.datetime.strptime(day["dateymd"], '%Y-%m-%d'))
            time = datetime.datetime.strptime(day["dateymd"], '%Y-%m-%d')
            fields = {
                "dailyconfirmed": day["dailyconfirmed"],
                "dailydeceased": day["dailydeceased"],
                "dailyrecovered": day["dailyrecovered"],
                "totalconfirmed": day["totalconfirmed"],
                "totaldeceased": day["totaldeceased"],
                "totalrecovered": day["totalrecovered"]
            }
            new_json_body = [
                {
                    "measurement": measurement,
                    "tags": {},
                    "time": str(time),
                    "fields": fields
                }
            ]

            insert_default_json(client, new_json_body)

print("Result: {0}".format(client.get_list_measurements()))
print(list(client.query("SELECT * FROM December")))