"""
This file will contain general resources in order to display the main application
"""
import colorama
from colorama import Fore
from datetime import datetime

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
    print("\tPlease, just insert the cases for this particular day, do not put the total number of cases! The values must be separated"
          "\n\tby spaces.")
    print("\n* update " + Fore.CYAN + "confirmed_today deceased_today recovered_today YYYY-MM-DD")
    print("\tPlease, just insert the cases for that particular day, do not put the total number of cases! The timestamp is obligatory in order"
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
        if len(data_list[0]) == 4 and check_int(data_list[0]) and len(data_list[1]) == 2 and check_int(data_list[1]) and len(data_list[2]) == 2 and check_int(
                data_list[2]):
            return True
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
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
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

    # List of months
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    return months[n_month - 1]


def get_current_time():
    """

    :return:
    """
    today = datetime.today()
    return str(datetime(today.year, today.month, today.day)).split()[0]



def get_previous_day(date):
    """
    Get the date of the previous day
    :param date:
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
