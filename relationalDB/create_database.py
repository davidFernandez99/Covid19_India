"""
RUN THIS FILE IN ORDER TO FILL THE RELATIONAL DATABASE

This file will contain the main logic to fill the database with the data which will be read from the TimeSeries Database.

Note: we have created the user and the database using pgAdmin.
"""
from relationalDB import resources as res
import utils

# We will compute for each month
for month in utils.MONTHS:

    # Compute and write the data to the postgres database
    res.compute_data_from_time_series(utils.TABLE_NAME, utils.TS_DB_NAME, month)

# Check pgAdmin to see the results :)
