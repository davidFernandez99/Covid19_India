"""
This file contains the main logic of the program in order to insert, delete or update a value to the TimeSeries
"""
import resources as res
from timeSeriesDB import resources as ts
from relationalDB import resources as rel_db

TABLE_NAME = "india_covid_"


def insert_point(command, client):
    """

    :param command:
    :param client:
    :return:
    """
    cmd_list = command.split()
    if res.check_format_insert_point(cmd_list):

        measurement_name = res.get_current_month()
        previous_point = []
        data = res.get_previous_day(res.get_current_time())
        while len(previous_point) == 0:
            print(data)
            previous_point = ts.get_point(client, measurement_name, data)

            data = res.get_previous_day(data)

        # Get previous point before inserting the current one in order to calculate the total_*
        previous_point = previous_point[0][0]
        print(f"previous point: {previous_point}")


        if len(previous_point) != 0:

            total_confirmed = int(previous_point["totalconfirmed"]) + int(cmd_list[1])
            total_deceased = int(previous_point["totaldeceased"]) + int(cmd_list[2])
            total_recovered = int(previous_point["totalrecovered"]) + int(cmd_list[3])

        else:
            total_confirmed = int(cmd_list[1])
            total_deceased = int(cmd_list[2])
            total_recovered = int(cmd_list[3])


        # Get points as dictionary
        points = {
            "dailyconfirmed": cmd_list[1],
            "dailydeceased": cmd_list[2],
            "dailyrecovered": cmd_list[3],
            "totalconfirmed": str(total_confirmed),
            "totaldeceased": str(total_deceased),
            "totalrecovered": str(total_recovered)
        }

        ts.insert_point(client, measurement_name, points)



        ########################################## RELATIONAL DB ################################################

        year = res.get_current_time().split("-")[0]
        table_name = TABLE_NAME + year
        # Tuple in order with all the info
        query_month_output = rel_db.get_data(table_name, dicc_conditions={"month": measurement_name})[0]
        days = int(query_month_output[-1]) + 1
        total_confirmed = int(query_month_output[4]) + int(cmd_list[1])
        total_deceased = int(query_month_output[5]) + int(cmd_list[2])
        total_recovered = int(query_month_output[6]) + int(cmd_list[3])
        avg_confirmed = total_confirmed / days
        avg_deceased = total_confirmed / days
        avg_recovered = total_confirmed / days

        rel_db.update_data(table_name, measurement_name, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered, days)


def update_point(command, client):
    cmd_list = command.split()
    if res.check_format_update_point(cmd_list):

        # TODO Get previous point before inserting the current one in order to calculate the total_*


        # Get measurement_name
        measurement_name = res.get_month_from_data(cmd_list[4])
        data = cmd_list[4]

        # Get previous point before inserting the current one in order to calculate the total_*
        previous_point = ts.get_point(client, measurement_name, data)[0]

        total_confirmed = int(previous_point["totalconfirmed"]) + int(cmd_list[1])
        total_deceased = int(previous_point["totaldeceased"]) + int(cmd_list[2])
        total_recovered = int(previous_point["totalrecovered"]) + int(cmd_list[3])

        # Get points as dictionary
        points = {
            "dailyconfirmed": cmd_list[1],
            "dailydeceased": cmd_list[2],
            "dailyrecovered": cmd_list[3],
            "totalconfirmed": str(total_confirmed),
            "totaldeceased": str(total_deceased),
            "totalrecovered": str(total_recovered)
        }

        ts.update_point(client, measurement_name, points, data)

        ########################################## RELATIONAL DB ################################################

        year = data.split("-")[0]
        table_name = TABLE_NAME + year
        current_point = ts.get_point(client, measurement_name, data)[0]


        # Tuple in order with all the info
        query_month_output = rel_db.get_data(table_name, dicc_conditions={"month": measurement_name})[0]
        days = int(query_month_output[-1])

        # Get curren total for the month, sum the new value for the day in the month and substract old value that was for this day
        total_confirmed = int(query_month_output[4]) + int(cmd_list[1]) - current_point["dailyconfirmed"]
        total_deceased = int(query_month_output[5]) + int(cmd_list[2]) - current_point["dailydeceased"]
        total_recovered = int(query_month_output[6]) + int(cmd_list[3]) - current_point["dailyrecovered"]
        avg_confirmed = total_confirmed / days
        avg_deceased = total_confirmed / days
        avg_recovered = total_confirmed / days

        rel_db.update_data(table_name, measurement_name, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered, days)


def delete_point(command, client):
    cmd_list = command.split()
    if res.check_format_delete_point(cmd_list):

        # TODO Get previous point before inserting the current one in order to calculate the total_*

        # Get measurement_name
        measurement_name = res.get_month_from_data(cmd_list[1])

        # TODO DELETE
        pass
