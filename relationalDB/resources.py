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
        conn = psycopg2.connect(database="covid_world", user="postgres", password="postgres")
        return conn
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
        print("Could not connect to the database")
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


def get_data(table_name, atr_tuple=None, dicc_conditions=None):
    """
    Get data from a table
    :param table_name: Name of the table where we will select the data
    :param atr_list: Tuple of attributes que want to select
    :param dicc_conditions: Dictionary of conditions with format key = column, value = value column
    :return: List of data selected
    """
    conn = connect_postgres()

    # If we could not connected to the database we will exit
    if not conn:
        print("Could not connect to the database")
        return

    # Like a pointer to the database
    cur = conn.cursor()

    try:
        columns_select = '*'
        if atr_tuple:
            columns_select = atr_tuple
        if dicc_conditions:
            condition = ''
            keys = list(dicc_conditions.keys())
            condition += f"{keys[0]}='{dicc_conditions[keys[0]]}'"
            for key_id in range(1, len(keys)):
                key = list(dicc_conditions.keys())[key_id]
                condition += f"AND {key}='{dicc_conditions[key]}'"
            print(f"SELECT {columns_select} FROM {table_name} WHERE {condition};")
            cur.execute(f"SELECT {columns_select} FROM {table_name} WHERE {condition};")
        else:
            cur.execute(f"SELECT {columns_select} FROM {table_name};")

        return list(cur.fetchall())[0]
    except Exception as e:
        print(e)


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

        lista_tablas = get_data('information_schema.tables', ('table_name'), {'table_schema': 'public'})
        if lista_tablas:
            lista_tablas = [tupla[0] for tupla in lista_tablas]
        else:
            lista_tablas = []
        print(f"lista tablas: {lista_tablas}")

        # init
        dicc_years = {}

        # Get the data we inserted in the time series database of each month
        query_output = res.get_month_data_time_series(client, month)

        # Compute data
        for data in query_output:
            # Check the year of the data
            time = data['time'].split("-")[0]

            # If is a new year of this month we creat a new key for this year with the values, we need this cause we
            # store all the years data in the same mesuarement in influxdb
            if time not in list(dicc_years.keys()):
                dicc_years[time] = {'total_confirmed': 0, 'total_deceased': 0, 'total_recovered': 0, 'days': 0,
                                    'avg_confirmed': 0, 'avg_deceased': 0, 'avg_recovered': 0}
                if table_name+time not in lista_tablas:
                    create_table(table_name+time)
                    lista_tablas.append(table_name+time)
            dicc_years[time]['total_confirmed'] += int(data['dailyconfirmed'])
            dicc_years[time]['total_deceased'] += int(data['dailydeceased'])
            dicc_years[time]['total_recovered'] += int(data['dailyrecovered'])
            dicc_years[time]['days'] += 1

        for key in list(dicc_years.keys()):
            days = dicc_years[key]['days']
            dicc_years[key]['avg_confirmed'] = round(dicc_years[key]['total_confirmed'] / days, 2)
            dicc_years[key]['avg_deceased'] = round(dicc_years[key]['total_deceased'] / days, 2)
            dicc_years[key]['avg_recovered'] = round(dicc_years[key]['total_recovered'] / days, 2)
            # Write the data into Table
            insert_data(table_name+key, month, dicc_years[key]['avg_confirmed'], dicc_years[key]['avg_deceased'],
                        dicc_years[key]['avg_recovered'], dicc_years[key]['total_confirmed'],
                        dicc_years[key]['total_deceased'], dicc_years[key]['total_recovered'], days)


print(get_data('india_covid_2020', dicc_conditions={"month": 'May'}))