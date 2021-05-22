"""
This file will create a new database into InfluxDB and will insert a json body with all data to fill the database.
"""
import os
from influxdb import InfluxDBClient
from timeSeriesDB import resources as res

# define database name
DB_NAME = "weather_db"

# List of months
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

# Get path of the Json directory
dir_path = os.path.dirname(os.path.realpath(__file__)) + '/data'

# Get all json files from the directory
json_files = [dir_path + f'/{f}' for f in os.listdir(dir_path)]


# Create new client to connect with InfluxDB
try:
    client = InfluxDBClient(host="localhost", port=8086)
except Exception as e:
    print(e)
else:
    # Create a new database
    res.create_new_database(client, DB_NAME)

    # Show databases
    res.show_databases(client)

    # Select database
    res.select_database(client, DB_NAME)

    # Empty database if it was already filled
    for month in months:
        res.drop_measurement(client, month)

    # Check that it is empty
    res.show_measurements(client)

    # Insert all json files
    for file in json_files:
        res.create_and_write_json(client, file)

    # Check that the measurements have been created successfully
    res.show_measurements(client)
