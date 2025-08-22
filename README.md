# Data-Cleaner
A Python script to clean data from a CSV file and load it into a database.

# Class: DataCleaner

# Parameters:
dataframe_path — Path to the CSV file to be cleaned.
dbpath — Path to the database file for storing cleaned data.

# Behavior:
Loads the CSV file from dataframe_path into the df attribute (a pandas DataFrame).
Connects to the database specified by dbpath.

# Methods and Their Functionalities
# 1.clean_data()
Removes columns where more than 5% of the values are null.
Fills remaining null values with the mean of their respective columns.

# 2.identify_columns()
Analyzes each column and generates a JSON file recommending:
Columns suitable for SELECT queries.
Columns suitable for GROUP BY queries.
Columns to avoid.

# 3.create_view()
Reads the generated JSON file and creates a database view based on the recommendations.

# 4.load_to_sql()
Loads the cleaned DataFrame (df) into the connected database.

# 5.run_query(query)
Executes a SQL query on the connected database and returns the result.
