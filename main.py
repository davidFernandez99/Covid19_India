from influxdb import InfluxDBClient
import resources as res
import controller as contr
import utils


def main():
    """
    Program starting and main logic
    :return:
    """
    # Create new client to connect with InfluxDB
    client = InfluxDBClient(host="localhost", port=8086)

    # Welcome!
    res.show_title()

    # We will select directly the database that the client will use
    client.switch_database(utils.TS_DB_NAME)

    # Start!
    contr.run(client)

    # Goodbye!
    res.exit_text()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
