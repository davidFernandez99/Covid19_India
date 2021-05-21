import resources as res
from influxdb import InfluxDBClient

"""
insert -> TimeSeries (a,b,c,d,e,f)
Update -> SQL (a,b,c,d,e,f)

"""
# Create new client to connect with InfluxDB
try:
    client = InfluxDBClient(host="localhost", port=8086)
except Exception as e:
    print(e)
else:
    # Select database
    client.switch_database("weather_db")

    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November",
              "December"]
    total_confirmed = 0
    total_deceased = 0
    total_recovered = 0
    days = 0
    for month in months:
        query_output = res.get_data_time_series(client, month)
        for data in query_output:
            total_confirmed += data['dailyconfirmed']
            total_deceased += data['dailydeceased']
            total_recovered += data['dailyrecovered']
            days += 1
        avg_confirmed = total_confirmed/days
        avg_deceased = total_deceased/days
        avg_recovered = total_recovered/days
        res.insert_data_relational_db(month, avg_confirmed, avg_deceased, avg_recovered, total_confirmed, total_deceased, total_recovered)
