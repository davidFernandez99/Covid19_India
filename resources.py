import datetime
import psycopg2

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


def get_data_time_series(client, month):
    return list(client.query(f"SELECT * FROM {month}"))[0]


def month_name(n):
    months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
    return months[n-1]


def create_table(name_table):
    try:
        conn = psycopg2.connect(database="india_covid", user="postgres", password="postgres")
    except Exception as e:
        print(e)
    else:
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE TABLE {name_table} (month text PRIMARY KEY, avg_confirmed float, avg_deceased float, avg_recovered float, "
                        f"total_confirmed integer, total_deceased integer, total_recovered integer);")
        except Exception as e:
            print(e)

        conn.commit()  # <--- makes sure the change is shown in the database
        conn.close()
        cur.close()


def insert_data_relational_db(month, table_name, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered):

    try:
        conn = psycopg2.connect(database="india_covid", user="postgres", password="postgres")
    except Exception as e:
        print(e)
    else:
        cur = conn.cursor()
        try:
            cur.execute(f"INSERT INTO {table_name} VALUES ('{month}', {avg_confirmed}, {avg_deceased}, {avg_recovered}, {total_confirmed}, {total_deceased}"
                        f",{total_recovered});")
        except Exception as e:
            print(e)

        conn.commit()  # <--- makes sure the change is shown in the database
        conn.close()
        cur.close()


def delete_data_relational_db(month, table_name):

    try:
        conn = psycopg2.connect(database="india_covid", user="postgres", password="postgres")
    except Exception as e:
        print(e)
    else:
        cur = conn.cursor()
        try:
            cur.execute(f"DELETE FROM {table_name} WHERE month='{month}';")
        except Exception as e:
            print(e)

        conn.commit()  # <--- makes sure the change is shown in the database
        conn.close()
        cur.close()


def update_data_relational_db(month, table_name, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered):

    try:
        conn = psycopg2.connect(database="india_covid", user="postgres", password="postgres")
    except Exception as e:
        print(e)
    else:
        cur = conn.cursor()
        try:
            cur.execute(f"UPDATE {table_name} SET avg_confirmed={avg_confirmed}, avg_deceased={avg_deceased}, avg_recovered={avg_recovered}, "
                        f"total_confirmed={total_confirmed}, total_deceased={total_deceased}, total_recovered={total_recovered} WHERE month='{month}';")
        except Exception as e:
            print(e)

        conn.commit()  # <--- makes sure the change is shown in the database
        conn.close()
        cur.close()


#insert_data_relational_db("may", "india_data", 1,2,3,4,5,6)
#update_data_relational_db("may", "india_data",10,20,30,40,50,60)
#delete_data_relational_db("may", "india_data")