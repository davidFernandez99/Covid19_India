"""
This file contains the main logic of the program in order to insert, delete or update a value to the TimeSeries
"""
import resources as res
from timeSeriesDB import resources as ts
from relationalDB import resources as rel_db
from colorama import Fore
import utils


def insert_point(command, client):
    """
    Main logic to insert a new point in the InfluxDB. Based on the data entered by the user, we need to get the measurement name
    where we want to insert the point and the previous point in order to update the total fields (since they are not asked to the user
    to insert them). Once we have computed and checked that all is in the correct format we insert the point in the corresponding
    measurement with the current time.

    Afterwards, we need to update the values for that specific month in the relational database. For that we need to get the previous data
    and update each field taking in account the new point inserted. For that, we will add +1 in total days (this is like total points
    for that month). Moreover, we will add to the corresponding total_* fields the values inserted by the user. For example:

    WHERE:
        total_recovered -> the new value of total recovered
        total_previous_recovered -> the value we got by doing a query to the database for the month we want to update the value
        recovered_today -> the amount of people that have recovered only today (the inserted point)

    total_recovered = total_previous_recovered + recovered_today

    Furthermore, we need also to recompute the average values. However, this will be easy since we have all the data. We only need to
    divide each total_* field by the total days. For example:

    avg_confirmed = total_recovered / total_days

    Note: we can not use the total*  values from the Time Series DB since they are the total accumulative. In the relational DB
    we have the total_* value for only that specific month, it is not accumulative. This is why we will have months with bigger
    numbers than others.

    :param command: the command introduced by the user where we have the values for today that will be inserted in the databases
    :param client: the client which is connected to InfluxDB and will be used to interact with the database
    :return:
    """
    # Let's split the command by spaces and get in a list format so it will be easy to process
    cmd_list = command.split()

    # Check that each value inserted is in the correct format
    if res.check_format_insert_point(cmd_list):

        """
         ########################################## TIME SERIES DB ########################################## 
        """

        # Get the month which will be the measurement_name where the point will be inserted
        measurement_name = res.get_current_month()

        # Get the previous point before inserting the current one in order to calculate the total_* fields
        previous_point = res.get_previous_point(client, measurement_name)

        # Computing the total_* fields using the previous point
        total_confirmed = int(previous_point["totalconfirmed"]) + int(cmd_list[1])
        total_deceased = int(previous_point["totaldeceased"]) + int(cmd_list[2])
        total_recovered = int(previous_point["totalrecovered"]) + int(cmd_list[3])

        # Get points as dictionary
        points = res.create_fields_dict(cmd_list[1], cmd_list[2], cmd_list[3], str(total_confirmed), str(total_deceased), str(total_recovered))

        # Now we have all the fields in the correct format so we insert them in the database
        ts.insert_point(client, measurement_name, points)


        """
         ########################################## RELATIONAL DB ########################################## 
        """

        # Get in which table we have to make the changes
        year = res.get_current_time().split("-")[0]
        table_name = utils.TABLE_NAME + year

        # Get the information we had before for that specific month, in order to update it
        query_month_output = rel_db.get_data(table_name, dicc_conditions={"month": measurement_name})

        # We are inserting one more data so we have to sum 1 to total days
        days = int(query_month_output[-1]) + 1

        # Compute total_* using the information we got from the query for the specific month
        total_confirmed = int(query_month_output[4]) + int(cmd_list[1])
        total_deceased = int(query_month_output[5]) + int(cmd_list[2])
        total_recovered = int(query_month_output[6]) + int(cmd_list[3])

        # Update the average values using the new values for that month
        avg_confirmed = round(total_confirmed / days, 2)
        avg_deceased = round(total_deceased / days, 2)
        avg_recovered = round(total_recovered / days, 2)

        # Update the data in the relational database
        rel_db.update_data(table_name, measurement_name, avg_confirmed, avg_deceased, avg_recovered, total_confirmed,
                           total_deceased, total_recovered, days)


def update_point(command, client):
    """
    Main logic to update a new point in the InfluxDB. Based on the data entered by the user, we need to get the measurement name
    where we want to update the point and the previous point in order to update the total fields (since they are not asked to the user
    to insert them). Once we have computed and checked that all is in the correct format we update the point in the corresponding
    measurement with the time specified by the user.

    Once we updated that specific point, it is time to do some extra work. Since now the total value has changed, we need to update
    each total_value for the next points!! This means all the points from that day specified by the user to today (or current day).
    So we will add or subtract the difference to the each total field of the next points. Look at RECALCULATE THE TOTAL OF EACH NEXT POINT
    to understand better what are we doing.

    Afterwards, we need to update the values for that specific month in the relational database. For that we need to get the previous data
    and update each field taking into account the new values of the updated point. For that, we will take the same number for total days
    (this is like total points for that month). Moreover, we will update the corresponding total_* fields with the values inserted by the user
    and subtract the value that was before. For example:

    WHERE:
        total_recovered -> the new value of total recovered
        total_previous_recovered -> the value we got by doing a query to the database for the month we want to update the value
        recovered_new_point -> the amount of people that have recovered that specific day (the updated point)
        recovered_old_point -> the amount of people that was recovered that specific day (the old point)

    total_recovered = total_previous_recovered + recovered_new_point - recovered_old_point

    Furthermore, we need also to recompute the average values. However, this will be easy since we have all the data. We only need to
    divide each total_* field by the total days. For example:

    avg_confirmed = total_recovered / total_days

    Note: we can not use the total*  values from the Time Series DB since they are the total accumulative. In the relational DB
    we have the total_* value for only that specific month, it is not accumulative. This is why we will have months with bigger
    numbers than others.

    :param command: the command introduced by the user where we have the values for today that will be inserted in the databases
    :param client: the client which is connected to InfluxDB and will be used to interact with the database
    :return:
    """
    # Let's split the command by spaces and get in a list format so it will be easy to process
    cmd_list = command.split()

    # Check that each value inserted is in the correct format
    if res.check_format_update_point(cmd_list):

        """
         ########################################## TIME SERIES DB ########################################## 
        """

        # Get the month which will be the measurement_name where the point will be updated
        measurement_name = res.get_month_from_data(cmd_list[4])

        # Get the date that identifies the point in the measurement, so we can update that point
        date = cmd_list[4]

        # Get the point that will be updated
        current_point = ts.get_point(client, measurement_name, date)

        # The point might not be in the database!
        if len(current_point) == 0:
            print(Fore.RED + "Error! Can't update cause this day is not stored in the database.")
            return
        else:
            # It is a dictionary inside a list, inside another list (if not empty)
            current_point = current_point[0][0]

        # Get previous point before updating the current one in order to calculate the total_*
        previous_point = res.get_previous_point(client, measurement_name, date)

        # Computing the total_* fields using the previous point
        total_confirmed = int(previous_point["totalconfirmed"]) + int(cmd_list[1])
        total_deceased = int(previous_point["totaldeceased"]) + int(cmd_list[2])
        total_recovered = int(previous_point["totalrecovered"]) + int(cmd_list[3])

        # Get points as dictionary
        points = res.create_fields_dict(cmd_list[1], cmd_list[2], cmd_list[3], str(total_confirmed), str(total_deceased), str(total_recovered))

        # Now we have all the fields in the correct format so we update them in the database
        ts.update_point(client, measurement_name, points, date)

        # --------------- RECALCULATE THE TOTAL OF EACH NEXT POINT ---------------
        #
        #   new_total_next_point = old_total_next_point + (new_total_current_point - old_total_current_point)
        #                                                 |__________________________________________________|
        #                                                                          |
        #                                               We are passing this part of the formula to the function
        #

        # Since we updated a point we need to compute the difference to sum or subtract to the total value
        res.recompute_total_fields_next_points(client, measurement_name, date,
                                               total_confirmed - int(current_point["dailyconfirmed"]),
                                               total_deceased - int(current_point["dailydeceased"]),
                                               total_recovered - int(current_point["dailyrecovered"]))


        """
         ########################################## RELATIONAL DB ########################################## 
        """

        # Get in which tables we have to make the changes
        year = date.split("-")[0]
        table = utils.TABLE_NAME + year

        query_month_output = rel_db.get_data(table, dicc_conditions={"month": measurement_name})

        # We are just updating a point that already exists so there is no need to modify the number of total days (== total points in that month)
        days = query_month_output[-1]

        res.recompute_values_for_month(table, measurement_name, days,
                                       int(query_month_output[4]) + int(cmd_list[1]) - int(current_point["dailyconfirmed"]),
                                       int(query_month_output[5]) + int(cmd_list[2]) - int(current_point["dailydeceased"]),
                                       int(query_month_output[6]) + int(cmd_list[3]) - int(current_point["dailyrecovered"]))


def delete_point(command, client):
    """
    Main logic to delete a point in the InfluxDB. Based on the data entered by the user, we need to get the measurement name
    where we want to delete the point.

    Once we deleted that specific point, it is time to do some extra work. Since now the total value has changed, we need to update
    each total_value for the next points!! This means all the points from that day specified by the user to today (or current day).
    So we will subtract that points each total value to the each total field of the next points. Look at RECALCULATE THE TOTAL OF EACH NEXT POINT
    to understand better what are we doing.

    Afterwards, we need to update the values for that specific month in the relational database. For that we need to get the previous data
    and update each field taking into account that we deleted a point. For that, we will subtract 1 for total days since we deleted a day
    (this is like total points for that month). Moreover, we will update the corresponding total_* fields with the values inserted by the user
    and subtract the value that was before. For example:

    WHERE:
        total_recovered -> the new value of total recovered
        total_previous_recovered -> the value we got by doing a query to the database for the month we want to update the value
        recovered_from_deleted_point -> the amount of people that have recovered that specific day but now we do not have that day
         anymore (the deleted point)

    total_recovered = total_previous_recovered - recovered_from_deleted_point

    Furthermore, we need also to recompute the average values. However, this will be easy since we have all the data. We only need to
    divide each total_* field by the total days. For example:

    avg_confirmed = total_recovered / total_days

    Note: we can not use the total*  values from the Time Series DB since they are the total accumulative. In the relational DB
    we have the total_* value for only that specific month, it is not accumulative. This is why we will have months with bigger
    numbers than others.

    :param command: the command introduced by the user where we have the values for today that will be inserted in the databases
    :param client: the client which is connected to InfluxDB and will be used to interact with the database
    :return:
    """
    # Let's split the command by spaces and get in a list format so it will be easy to process
    cmd_list = command.split()
    print(cmd_list)

    # Check that each value inserted is in the correct format
    if res.check_format_delete_point(cmd_list):

        """
         ########################################## TIME SERIES DB ########################################## 
        """

        # Get the month which will be the measurement_name where the point will be deleted
        measurement_name = res.get_month_from_data(cmd_list[1])

        # Get the date that identifies the point in the measurement, so we can delete that point
        date = cmd_list[1]

        # Get the point that will be deleted
        current_point = ts.get_point(client, measurement_name, date)
        print(current_point)

        if len(current_point) == 0:
            print(Fore.RED + "Error! Can't delete cause this day is not stored in the database.")
            return
        else:
            # It is a dictionary inside a list, inside another list (if not empty)
            current_point = current_point[0][0]

        # Delete the point
        ts.delete_data(client, measurement_name, date)

        # --------------- RECALCULATE THE TOTAL OF EACH NEXT POINT ---------------
        # Since we deleted a point we need to subtract the total value to each next point (since it is an accumulative value)
        res.recompute_total_fields_next_points(client, measurement_name, date,
                                               - int(current_point["dailyconfirmed"]),
                                               - int(current_point["dailydeceased"]),
                                               - int(current_point["dailyrecovered"]))

        """
         ########################################## RELATIONAL DB ########################################## 
        """

        # Get in which tables we have to make the changes
        year = date.split("-")[0]
        table = utils.TABLE_NAME + year

        query_month_output = rel_db.get_data(table, dicc_conditions={"month": measurement_name})

        # We deleted a point we need to modify the number of total days (== total points in that month)
        days = query_month_output[-1] - 1

        res.recompute_values_for_month(table, measurement_name, days,
                                       int(query_month_output[4]) - int(current_point["dailyconfirmed"]),
                                       int(query_month_output[5]) - int(current_point["dailydeceased"]),
                                       int(query_month_output[6]) - int(current_point["dailyrecovered"]))
