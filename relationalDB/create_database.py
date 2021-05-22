"""
This file will contain the main logic to fill the database with the data which will be read from the TimeSeries Database.

Note: we have created the user and the database using pgAdmin.
"""
from relationalDB import resources as res
from timeSeriesDB.create_database import DB_NAME

# define table name
TABLE_NAME = "india_covid"

# List of months
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]


# We will compute for each month
for month in months:

    # Compute and write the data to the postgres database
    res.compute_data_from_time_series(TABLE_NAME, DB_NAME, month)

# Check pgAdmin to see the results :)
