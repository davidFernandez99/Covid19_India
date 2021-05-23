"""
This file will contain general resources in order to display the main application
"""
import colorama
from colorama import Fore
from datetime import datetime
from timeSeriesDB import resources as res_ts
from relationalDB import resources as res_rl
import utils

# Once is enough
colorama.init(autoreset=True)


def show_title():
    """
    Print the title
    :return:
    """
    print(Fore.CYAN + "\n##### COVID UB TIME SERIES #####\n")


def exit_text():
    """
    Print the farewell
    :return:
    """
    print("\nGOODBYE! See you next time ;)")


def show_help_commands():
    """
    Print the commands that can be used in this demo
    :return:
    """
    print("\n###### COMMANDS ######")
    print("* insert " + Fore.CYAN + "confirmed_today deceased_today recovered_today")
    print(
        "\tPlease, just insert the cases for this particular day, do not put the total number of cases! The values must be separated"
        "\n\tby spaces.")
    print("\n* update " + Fore.CYAN + "confirmed_today deceased_today recovered_today YYYY-MM-DD")
    print(
        "\tPlease, just insert the cases for that particular day, do not put the total number of cases! The timestamp is obligatory in order"
        "\n\tto be able do identify the point in the TimeSeriesDB and in the relational DB.")
    print("\n* delete " + Fore.CYAN + "YYYY-MM-DD")
    print("\tInsert the timestamp, and the point to be removed will be identified.")
    print("\n* exit, close, quit")
    print("\tWrite one of the options above to finish the program.")


def show_error_command(command):
    """
    Print in red color a message error
    :param command: the command that produced the error
    :return:
    """
    print(Fore.RED + "Error! Command [" + command + "] does not exist or is not implemented yet.")


def check_int(n):
    """
    Check if the string value can be converted to integer

    :param n: string value to be converted to integer
    :return: True if it can be converted; False otherwise
    """
    try:
        int(n)
        return True
    except Exception as e:
        print(e)
        return False


def check_data_format(data_time):
    """
    Check if the data wrote by the user is in the correct format

    :param data_time:
    :return:
    """
    data_list = data_time.split("-")
    if len(data_list) == 3:
        if len(data_list[0]) == 4 and check_int(data_list[0]) and len(data_list[1]) == 2 and check_int(data_list[1]) and len(data_list[2]) == 2 and check_int(data_list[2]):
            return True
    print("Error data format! Please follow this format: YYYY-MM-DD")
    return False


def check_format_insert_point(cmd_list):
    """
    Check command format for inserting a point

    :param cmd_list:
    :return: True if the format is correct; False otherwise
    """
    if len(cmd_list) == 4:
        if cmd_list[0] == "insert" and check_int(cmd_list[1]) and check_int(cmd_list[2]) and check_int(cmd_list[3]):
            return True
    print("Incorrect format!")
    return False


def check_format_update_point(cmd_list):
    """
    Check command format for inserting a point

    :param cmd_list:
    :return: True if the format is correct; False otherwise
    """
    if len(cmd_list) == 5:
        if cmd_list[0] == "update" and check_int(cmd_list[1]) and check_int(cmd_list[2]) and check_int(cmd_list[3]) and check_data_format(cmd_list[4]):
            return True
    print("Incorrect format!")
    return False


def check_format_delete_point(cmd_list):
    """
    Check command format for inserting a point

    :param cmd_list:
    :return: True if the format is correct; False otherwise
    """
    if len(cmd_list) == 2:
        if cmd_list[0] == "delete" and check_data_format(cmd_list[1]):
            return True
    print("Incorrect format!")
    return False


def get_month_from_data(date_time):
    """
    Get the month from the data field inserted by the user

    :param date_time: data string in format YYYY-MM-DD
    :return: a string with the month name
    """
    # Get the month as integer number
    n_month = int(date_time.split("-")[1])

    # List of months
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
              "November", "December"]
    return months[n_month - 1]


def get_current_month():
    """
    Get current month

    :return: a string with the month name
    """

    today = datetime.today()
    datem = datetime(today.year, today.month, 1)

    # Get the month as integer number
    n_month = int(str(datem).split("-")[1])

    return utils.MONTHS[n_month - 1]


def get_current_time():
    """
    Get the current time in the following format YYYY-MM-DD

    :return: a string with the time described above
    """
    today = datetime.today()
    return str(datetime(today.year, today.month, today.day)).split()[0]


def get_current_time_format():
    """

    :return:
    """
    today = datetime.today()
    return datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d')


def get_previous_day(date):
    """
    Get the date of the previous day

    :param date: the day of which we want to calculate the previous date
    :return: Previous day of date
    """
    string_date = str(date)
    time_list = string_date.split("-")
    year = int(time_list[0])
    month = int(time_list[1])
    day = int(time_list[2][0:2])
    if day == 1 and month == 1:
        day_return = 31
        month_return = 12
        year_return = year - 1
    elif day == 1 and month != 1:
        month_return = month - 1
        day_return = get_num_days_for_month(month_return, year)
        year_return = year
    else:
        day_return = day - 1
        month_return = month
        year_return = year

    return datetime.strptime(f"{year_return}-{month_return}-{day_return}", '%Y-%m-%d')


def get_next_day(date):
    """
    Get the date of the next day

    :param date: the day of which we want to calculate the next date
    :return: Next day of date
    """
    string_date = str(date)
    time_list = string_date.split("-")
    year = int(time_list[0])
    month = int(time_list[1])
    day = int(time_list[2][0:2])
    days_month = get_num_days_for_month(month, year)
    if day == days_month and month == 12:
        day_return = 1
        month_return = 1
        year_return = year + 1
    elif day == days_month and month != 12:
        month_return = month + 1
        day_return = 1
        year_return = year
    else:
        day_return = day + 1
        month_return = month
        year_return = year

    return datetime.strptime(f"{year_return}-{month_return}-{day_return}", '%Y-%m-%d')


def get_num_days_for_month(month_num, year_num):
    """
    Get number of days for the month num

    :param month_num: Num of the month (1-12)
    :param year_num: Num of the year
    :return: Number of days in the month number
    """
    dict_days_month = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    try:
        if month_num != 2:
            return dict_days_month[month_num]
        else:
            leap = 0
            if year_num % 400 == 0:
                leap = 1
            elif year_num % 100 == 0:
                leap = 0
            elif year_num % 4 == 0:
                leap = 1
            return 28 + leap
    except:
        return -1


def get_previous_point(client, measurement_name, date=None):
    """
    Get the previous point where we have data inserted starting from today

    :param client: the client that connects with InfluxDB and allow us to interact with the database
    :param measurement_name:
    :param date:
    :return:
    """
    previous_point = []

    if not date:
        date = get_current_time()

    data = get_previous_day(date)

    while len(previous_point) == 0:
        previous_point = res_ts.get_point(client, measurement_name, data)
        data = get_previous_day(data)

    return previous_point[0][0]


def create_fields_dict(d_conf, d_dec, d_rec, t_conf, t_dec, t_rec):
    """
    Create a dictionary with the fields already computed

    :param d_conf: number of confirmed during this day
    :param d_dec: number of deceased during this day
    :param d_rec: number of recovered during this day
    :param t_conf: total number of confirmed
    :param t_dec: total number of deceased
    :param t_rec: total number of recovered
    :return: a dictionary with the corresponding fields
    """
    points = {
        "dailyconfirmed": d_conf,
        "dailydeceased": d_dec,
        "dailyrecovered": d_rec,
        "totalconfirmed": t_conf,
        "totaldeceased": t_dec,
        "totalrecovered": t_rec
    }
    return points


def recompute_total_fields_next_points(client, measurement_name, date, d_conf, d_dec, d_rec):
    """
    Recompute the total_fields for the next points. The total fields are the following ones:
        total_confirmed
        total_deceased
        total_recovered

    :param client: the client that connects with InfluxDB and allow us to interact with the database
    :param measurement_name: the measurement name where we want to update the data
    :param date: from where we will "start" to recompute the data
    :param d_conf: the confirmed cases to be applied
    :param d_dec: the deceased cases to be applied
    :param d_rec: the recovered cases to be applied
    :return:
    """
    # Get next day
    next_date = get_next_day(date)

    # Current day
    today_date = get_current_time_format()

    # We will do it till we get to the last point (it is supposed to be the current day)
    while next_date != today_date:

        # Let's get the next point
        next_point = res_ts.get_point(client, measurement_name, next_date)
        print(next_point)

        # There might be days that there are no points (you never know)
        if len(next_point) != 0:
            next_point = next_point[0][0]

            # Create dictionary with the necessary fields
            points = create_fields_dict(next_point["dailyconfirmed"], next_point["dailydeceased"], next_point["dailyrecovered"],
                                        str(int(next_point["totalconfirmed"]) + d_conf),
                                        str(int(next_point["totaldeceased"]) + d_dec),
                                        str(int(next_point["totalrecovered"]) + d_rec))

            # Update that point
            res_ts.update_point(client, measurement_name, points, next_date)

        # Once we finished to update the point for that day, let's get the next one
        next_date = get_next_day(next_date)


def recompute_values_for_month(table, month, days, total_confirmed, total_deceased, total_recovered):
    """
    Recompute the values for the specific month in the relational Database

    :param table: table name where the month it is located
    :param month: the month that we want to recompute the values
    :param days: number of days (or points)
    :param total_confirmed: total confirmed cases (value already computed)
    :param total_deceased: total deceased cases (value already computed)
    :param total_recovered: total recovered cases (value already computed)
    :return:
    """

    # Update the average values using the new values for that month
    avg_confirmed = round(total_confirmed / days, 2)
    avg_deceased = round(total_deceased / days, 2)
    avg_recovered = round(total_recovered / days, 2)

    # Update the data in the relational database
    res_rl.update_data(table, month, avg_confirmed, avg_deceased, avg_recovered, total_confirmed,
                       total_deceased, total_recovered, days)
