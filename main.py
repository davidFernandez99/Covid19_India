from influxdb import InfluxDBClient
import resources as res
import controller as contr


def main():
    """
    Program starting and main logic
    :return:
    """
    # Create new client to connect with InfluxDB
    client = InfluxDBClient(host="localhost", port=8086)

    # Welcome!
    res.show_title()

    client.switch_database("weather_db")

    # Start!
    contr.run(client)

    # Goodbye!
    res.exit_text()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
