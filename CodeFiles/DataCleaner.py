import json
import sqlite3
from venv import logger
import pandas


class DataCleaner:

    def __init__(self, dataframe_path, dbpath):
        # Load CSV file into pandas dataframe
        self.df = pandas.read_csv(dataframe_path)
        # Set database path
        self.database_path = dbpath
        # Create SQLite connection
        self.conn = sqlite3.connect(self.database_path)

    def clean_data(self) -> None:
        # Calculate percentage of null values per column
        null_percentage = (self.df.isnull().sum() / len(self.df)) * 100
        logger.info(null_percentage)

        # Log columns to be dropped (those with >= 5% null values)
        logger.info("Columns to be Dropped \n")
        logger.info(null_percentage[null_percentage >= 5])

        # Drop columns with >= 5% null values
        self.df.drop(columns=null_percentage[null_percentage >= 5].index, inplace=True)

        # Select numeric columns (float64 & int64)
        mean_columns = self.df.select_dtypes(include=["float64", "int64"]).columns
        # Calculate mean of numeric columns
        mean = self.df[mean_columns].mean()
        logger.info(mean.to_dict())

        # Fill null values with mean for numeric columns
        self.df.fillna(mean.to_dict(), inplace=True)

    def is_datetime(self, column_name) -> bool:
        # Check if column is datetime type
        logger.info("Checking the datatype of column " + column_name)
        try:
            pandas.to_datetime(self.df[column_name], errors="raise")
            logger.info(column_name + " is datetime column")
            return True
        except ValueError as e:
            # If conversion fails, column is not datetime
            return False

    def identify_columns(self) -> str:
        # Define dictionary for column categorization
        columnTypes = {
            "aggregation": self.df.select_dtypes(include=["number", "float64", "int64"]).columns.tolist(),
            "groupby": [],
            "avoid": []
        }

        # Iterate over object/string columns
        for col in self.df.select_dtypes(include=["object", "string"]).columns.tolist():
            if not self.is_datetime(col):
                logger.info(col + " added to groupby")
                columnTypes["groupby"].append(col)  # add to groupby if not datetime
            else:
                logger.info(col + " is avoided")
                columnTypes["avoid"].append(col)  # add to avoid if datetime

        # Save column categorization to JSON
        file_path = "column_data.json"
        with open(file_path, "w") as json_file:
            logger.debug("Started writing in  " + file_path)
            json.dump(columnTypes, json_file, indent=4)
            logger.debug("Written in " + file_path)
        return file_path

    def create_view(self, json_path, table) -> None:
        # Create SQL view from JSON column categorization
        logger.info("Creating view")
        isCreated = False
        logger.debug("creating a view")
        # Load JSON data
        logger.debug("Started reading in  " + json_path)
        with open(json_path, 'r') as file:
            data = json.load(file)

        # Build aggregate select clause
        select_aggregates = ",".join([f" AVG(`{col}`) as `avg_{col} `" for col in data["aggregation"]])
        # Build groupby select clause
        select_groupby = ",".join([f" `{col}` " for col in data["groupby"]])
        if select_groupby != "":
            fullClause = select_aggregates + " , " + select_groupby
        else:
            fullClause = select_aggregates + select_groupby
        while not isCreated:
            try:
                viewName = input('Enter view table Name: ')  # ask user for view name
                query = "create view " + viewName + " As select "
                # If groupby columns exist
                if select_groupby != "":
                    query += fullClause + f" from {table} group by " + select_groupby
                else:
                    query += fullClause + f" from {table}"

                # Execute query to create view
                logger.debug(query)
                self.conn.execute(query)
                self.conn.commit()
                isCreated = True
                logger.debug("View Query: " + query)
                logger.info("View Created")

            except sqlite3.OperationalError as e:
                # Handle SQL errors
                logger.error(e)
            except ValueError as e:
                # Handle value errors
                logger.error(e)

    def load_to_sql(self) -> str:
        # Load dataframe into SQL table
        logger.info("Started to load data to table")
        isCreated = False
        while not isCreated:
            table = input("\nEnter table name: ")  # ask user for table name
            try:
                # Write dataframe to SQL table
                self.df.to_sql(table, self.conn)
                self.conn.commit()
                isCreated = True
                return table
            except sqlite3.OperationalError as e:
                logger.error(e)
            except ValueError as e:
                logger.error(e)

    def run_query(self) -> None:
        # Interactive query execution
        isOn = True
        while isOn:
            query = input("Enter query or enter C to quit: ")  # user enters query
            if query != "C":
                try:
                    result = self.conn.execute(query)  # execute query
                    print(result.fetchall())  # print results
                except sqlite3.OperationalError as e:
                    logger.error(e)
            else:
                isOn = False  # exit loop if user enters C

    def automate(self) -> None:
        # Run full automated pipeline
        self.clean_data()  # step 1: clean data
        json_path = self.identify_columns()  # step 2: identify columns & save JSON
        input(
            "\nThe Json file with the column details is created at path " + json_path + " \n please check and update the columns as needed. Once updated hit enter \n")
        table = self.load_to_sql()  # step 3: load data into SQL table
        self.create_view(json_path, table)  # step 4: create view from JSON
        self.run_query()  # step 5: run queries interactively