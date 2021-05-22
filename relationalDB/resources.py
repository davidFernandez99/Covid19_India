"""
This file will contain all the functions which will allow us to interact with PostgreSQL (our relational DB).

Note: we have created the user and the database using pgAdmin.
"""
import psycopg2
from timeSeriesDB import resources as res
from influxdb import InfluxDBClient


def connect_postgres():
    """
    This function will create a session for us in order to work with the created database in PostgreSQL
    Since the database is already created, the values of these fields will be permanents

    :return: the connection which will allow us to interact with the database; None if we could not create a connection
    """
    try:
        return psycopg2.connect(database="india_covid", user="postgres", password="postgres")
    except Exception as e:
        print(e)
        return None


def commit_changes(conn, cur):
    """
    Commit and save changes of the operation we did

    :param conn: the connection that will allow us to interact with the database
    :param cur: the pointer to the database
    :return:
    """
    conn.commit()  # <--- makes sure the change is shown in the database
    conn.close()
    cur.close()


def create_table(table_name):
    """
    Create a new table to our relational database
    The fields can not be changed since they are related to the fields we have created in the Time Series Database

    Fields
    -------------------------------------------------------------------------------------
    month (text, PK) -> will be used to relate the relational DB with the measurements of the Time Series Database
    avg_confirmed (float) -> will contain the average number of confirmed cases in a month
    avg_deceased (float) -> will contain the average number of deceased cases in a month
    avg_recovered (float) -> will contain the average number of recovered cases in a month
    total_confirmed (integer) -> will contain the total number of confirmed cases in a month
    total_deceased (integer) -> will contain the total number of deceased cases in a month
    total_recovered (integer) -> will contain the total number of recovered cases in a month
    total_days (integer) -> will contain the total number of days that we have inserted data in a month

    The total_* fields will be useful in order to recompute the avg_* fields in case a point is modified or inserted in the TS database

    :param table_name: the name to be given to the table
    :return:
    """
    conn = connect_postgres()

    # If we could not connected to the database we will exit
    if not conn:
        return

    # Like a pointer to the database
    cur = conn.cursor()

    try:
        cur.execute(f"CREATE TABLE {table_name} (month text PRIMARY KEY, avg_confirmed float, avg_deceased float, avg_recovered float, "
                    f"total_confirmed integer, total_deceased integer, total_recovered integer, total_days integer);")
        print("Table created!")
    except Exception as e:
        print(e)
    else:
        commit_changes(conn, cur)


def insert_data(table_name, month, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered, total_days):
    """
    Insert the computed results for the specific month in the table

    :param table_name: the name of the table where we want to insert the data
    :param month: the name of the month which is a unique value
    :param avg_confirmed: the average number of confirmed cases for the month
    :param avg_deceased: the average number of deceased cases for the month
    :param avg_recovered: the average number of recovered cases for the month
    :param total_confirmed: the total number of confirmed cases for the month
    :param total_deceased: the total number of deceased cases for the month
    :param total_recovered: the total number of recovered cases for the month
    :param total_days: the total number of days for which we have data for that month
    :return:
    """
    conn = connect_postgres()

    # If we could not connected to the database we will exit
    if not conn:
        return

    # Like a pointer to the database
    cur = conn.cursor()

    try:
        cur.execute(f"INSERT INTO {table_name} VALUES ('{month}', {avg_confirmed}, {avg_deceased}, {avg_recovered}, {total_confirmed}, {total_deceased}"
                    f", {total_recovered}, {total_days});")
        print("Data inserted!")
    except Exception as e:
        print(e)
    else:
        commit_changes(conn, cur)


def update_data(table_name, month, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered, total_days):
    """
    Update the computed results for the specific month in the table

    :param table_name: the name of the table where we want to insert the data
    :param month: the name of the month which is a unique value
    :param avg_confirmed: the average number of confirmed cases for the month
    :param avg_deceased: the average number of deceased cases for the month
    :param avg_recovered: the average number of recovered cases for the month
    :param total_confirmed: the total number of confirmed cases for the month
    :param total_deceased: the total number of deceased cases for the month
    :param total_recovered: the total number of recovered cases for the month
    :param total_days: the total number of days for which we have data for that month
    :return:
    """
    conn = connect_postgres()

    # If we could not connected to the database we will exit
    if not conn:
        return

    # Like a pointer to the database
    cur = conn.cursor()

    try:
        cur.execute(f"UPDATE {table_name} SET avg_confirmed={avg_confirmed}, avg_deceased={avg_deceased}, avg_recovered={avg_recovered}, "
                    f"total_confirmed={total_confirmed}, total_deceased={total_deceased}, total_recovered={total_recovered}, total_days={total_days} "
                    f"WHERE month='{month}';")  # PK
        print("Data updated!")
    except Exception as e:
        print(e)
    else:
        commit_changes(conn, cur)


def delete_data(table_name, month):
    """
    Delete a row of the table by his PK

    :param table_name: the name of the table which contain the row we want to delete
    :param month: the PK that will allow to identify the row and delete it
    :return:
    """
    conn = connect_postgres()

    # If we could not connected to the database we will exit
    if not conn:
        return

    # Like a pointer to the database
    cur = conn.cursor()

    try:
        cur.execute(f"DELETE FROM {table_name} WHERE month='{month}';")  # PK
        print("Data deleted!")
    except Exception as e:
        print(e)
    else:
        commit_changes(conn, cur)


def compute_data_from_time_series(table_name, db_name, month):
    """
    Read data from the time series data and compute the necessary data in order to write to the relational table

    :param table_name: where we will write the data computed
    :param db_name: the name of the time series database from which we will collect the data
    :param month: the measurement name from which we will read the data and the PK to be writen in the relational database
    :return:
    """
    # Create new client to connect with InfluxDB
    try:
        client = InfluxDBClient(host="localhost", port=8086)
    except Exception as e:
        print(e)
    else:
        # Select database
        res.select_database(client, db_name)

        # init
        total_confirmed = 0
        total_deceased = 0
        total_recovered = 0
        days = 0

        # Get the data we inserted in the time series database of each month
        query_output = res.get_month_data_time_series(client, month)

        # Compute data
        for data in query_output:
            total_confirmed += data['dailyconfirmed']
            total_deceased += data['dailydeceased']
            total_recovered += data['dailyrecovered']
            days += 1
        avg_confirmed = total_confirmed / days
        avg_deceased = total_deceased / days
        avg_recovered = total_recovered / days

        # Write the data into Table
        insert_data(table_name, month, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered, days)
