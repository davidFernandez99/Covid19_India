"""
This file will contain all the functions which will allow us to interact with InfluxDB (our time series DB).
"""
import json
import datetime

from influxdb import InfluxDBClient


def create_new_database(client, db_name):
    """
    Create a new database in influxDB

    :param client: the client that connects with InfluxDB
    :param db_name: the name that will be given to the database
    :return:
    """
    try:
        client.create_database(db_name)
        print("Database created!")
    except Exception as e:
        print(e)


def show_databases(client):
    """
    Show all databases which are created in InfluxDB

    :param client: the client that connects with InfluxDB
    :return:
    """
    try:
        print(f"Databases: {client.get_list_database()}")
    except Exception as e:
        print(e)


def select_database(client, db_name):
    """
    Select the database with which we will work

    :param client: the client that connects with InfluxDB
    :param db_name: the database name which we is going to be selected
    :return:
    """
    try:
        client.switch_database(db_name)
        print(f"Database {db_name} selected!")
    except Exception as e:
        print(e)


def insert_point(client, measurement_name, points):
    """
    Insert points to our database by passing a list of points.

    :param client: the client that connects with InfluxDB
    :param measurement_name: the measurement name where we want to insert the point
    :param points:
    :return:
    """
    # Create a new json body with only the necessary data extracted and in the correct format
    new_json_body = [
        {
            "measurement": measurement_name,
            "tags": {},
            "fields": points
        }
    ]

    # Write the points to the database
    insert_json(client, new_json_body)


def update_point(client, measurement_name, points, time):
    """
    Update the value of a specific point from a specific measurement

    :param client: the client that connects with InfluxDB
    :param measurement_name: the measurement name where we want to update the point
    :param points:
    :param time:
    :return:
    """
    # Create a new json body with only the necessary data extracted and in the correct format
    new_json_body = [
        {
            "measurement": measurement_name,
            "tags": {},
            "time": str(time),
            "fields": points
        }
    ]

    # Write the points to the database
    insert_json(client, new_json_body)


def drop_database(client, db_name):
    """
    Delete a database from InfluxDB

    :param client: the client that connects with InfluxDB
    :param db_name: the database name which is going to be deleted
    :return:
    """
    try:
        client.drop_database(db_name)
        print(f"Database {db_name} deleted!")
    except Exception as e:
        print(e)


def drop_measurement(client, measurement_name):
    """
    Delete a measurement from the selected database

    :param client: the client that connects with InfluxDB
    :param measurement_name: the measurement name which is going to be deleted
    :return:
    """
    try:
        client.drop_measurement(measurement_name)
        print(f"Measurement {measurement_name} deleted!")
    except Exception as e:
        print(e)


def delete_data(client, measurement_name, time):
    """
    Delete a data from the selected database with a specific time
    :param client: the client that connects with InfluxDB
    :param measurement_name: the measurement name
    :param time: Time of the data we want to delete
    :return:
    """
    try:
        return client.query(f"DELETE FROM {measurement_name} WHERE (time='{time}');")
    except Exception as e:
        print(e)


def show_measurements(client):
    """
    Show all measurements of a specific database (must be selected)

    :param client: the client that connects with InfluxDB
    :return:
    """
    try:
        print(f"Measurements: {client.get_list_measurements()}")
    except Exception as e:
        print(e)


def insert_json(client, json_body):
    """
    Insert data to InfluxDB. The data must be writen in json protocol

    :param client: the client that connects with InfluxDB and allow us to interact with the database
    :param json_body: the json that will contain all the data that we will insert in our database
    :return:
    """

    try:
        client.write_points(json_body)
    except Exception as e:
        print(str(e))


def get_month_data_time_series(client, month):
    """
    Return a list of all the data we have saved in our influxDB for a certain month

    :param client: the client that connects with InfluxDB and allow us to interact with the database
    :param month: the month for which we want the data
    :return: a list of all data we got for the specific month
    """

    # For default it puts a list into another list, like this [[data]], so we return just [data]
    return list(client.query(f"SELECT * FROM {month}"))[0]


def create_and_write_json(client, json_file):
    """
    Read the given json file and extract only the necessary information in order to create a new json body with the correct format
    so it can be passed to a function which will write all the points in the database.

    :param client: the client that connects with InfluxDB and allow us to interact with the database
    :param json_file: the json file where we extract the data
    :return:
    """
    with open(json_file) as json_f:
        # Get all data from the json file and convert into a dictionary
        list_data = json.load(json_f)["cases_time_series"]

        # Each day will be a point
        for day in list_data:
            # Get data from dictionary
            measurement = day["date"].split()[1]
            time = datetime.datetime.strptime(day["dateymd"], '%Y-%m-%d')
            fields = {
                "dailyconfirmed": day["dailyconfirmed"],
                "dailydeceased": day["dailydeceased"],
                "dailyrecovered": day["dailyrecovered"],
                "totalconfirmed": day["totalconfirmed"],
                "totaldeceased": day["totaldeceased"],
                "totalrecovered": day["totalrecovered"]
            }

            # Create a new json body with only the necessary data extracted and in the correct format
            new_json_body = [
                {
                    "measurement": measurement,
                    "tags": {},
                    "time": str(time),
                    "fields": fields
                }
            ]

            # Write the points to the database
            insert_json(client, new_json_body)


def get_point(client, month, time):
    return list(client.query(f"SELECT * FROM {month} WHERE (time='{time}');"))



"""# Create new client to connect with InfluxDB
try:
    client = InfluxDBClient(host="localhost", port=8086)
except Exception as e:
    print(e)
else:
    # Select database
    select_database(client, "weather_db")
    print(get_month_data_time_series(client, "May"))
    delete_data(client, "May", "2020-05-01T00:00:00Z")
    print(get_month_data_time_series(client, "May"))"""
