import datetime

epoch = datetime.datetime.utcfromtimestamp(0)


def convert_to_milliseconds(dt):
    return (dt - epoch).total_seconds() * 1000


def insert_default_json(client, json_body):
    """
    Insert the default JSON that it is located in utils.py
    :param client: the client that connects with InfluxDB
    :param json_body: the json we will insert in our database
    :return:
    """

    try:
        # Should return True
        client.write_points(json_body)
        #print("JSON inserted successfully")
    except Exception as e:
        print(str(e))


def month_name(n):
    months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
    return months[n-1]
