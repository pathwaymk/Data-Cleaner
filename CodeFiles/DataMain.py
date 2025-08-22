import sqlite3

import pandas
import DataCleaner

dataFrame = pandas.read_csv("../Assets/sensor.csv")

# Total rows in the dataFrame
total = len(dataFrame.index)

columnName = "Sensor_Value_"

# Column have no name
dataFrame.columns.values[1] = "TimeStamp"

rowCount = {}

# Renaming all other columns
for i in range(2, 52):
    dataFrame.columns.values[i] = columnName + str(i-1)


dataFrame = dataFrame.drop(["Sensor_Value_16", "Sensor_Value_2"], axis=1)

for column in dataFrame.columns:
    if column not in ("TimeStamp","Unnamed: 0","machine_status"):
        meanValue = dataFrame[column].mean()
        dataFrame.fillna({column: meanValue}, inplace=True)

for columns in dataFrame.columns:
    nullC = int(dataFrame[columns].isna().sum())
    rowCount[columns] = {"Total": total, "NullC": nullC, "Percentage": (nullC/total)*100}

rowsWithNull = {key : value for key, value in rowCount.items() if value["Percentage"] > 0}

groupByDf = dataFrame.groupby('machine_status').agg({'Sensor_Value_11':'mean', 'Sensor_Value_15':'mean', 'Sensor_Value_25':'mean'})

print(groupByDf)

# conn = sqlite3.connect('Sensor.db')
# dataFrame.to_sql("Sensor", conn)