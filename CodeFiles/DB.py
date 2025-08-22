import sqlite3
import pandas

# dataFrame = pandas.read_csv("../Assets/sensor.csv")
conn = sqlite3.connect('Sensor.db')
# dataFrame.to_sql("Sensor", conn)

result = conn.execute("SELECT machine_status, AVG(Sensor_Value_11), AVG(Sensor_Value_15) FROM sensor GROUP BY machine_status")

print(result.fetchall())

view_query = ("""create view sensor_summary as SELECT
    machine_status,
    AVG(Sensor_Value_1) AS avg_sensor_1,
    AVG(Sensor_Value_3) AS avg_sensor_3,
    AVG(Sensor_Value_4) AS avg_sensor_4,
    AVG(Sensor_Value_5) AS avg_sensor_5,
    AVG(Sensor_Value_6) AS avg_sensor_6,
    AVG(Sensor_Value_7) AS avg_sensor_7,
    AVG(Sensor_Value_8) AS avg_sensor_8,
    AVG(Sensor_Value_9) AS avg_sensor_9,
    AVG(Sensor_Value_10) AS avg_sensor_10,
    AVG(Sensor_Value_11) AS avg_sensor_11,
    AVG(Sensor_Value_12) AS avg_sensor_12,
    AVG(Sensor_Value_13) AS avg_sensor_13,
    AVG(Sensor_Value_14) AS avg_sensor_14,
    AVG(Sensor_Value_15) AS avg_sensor_15,
    AVG(Sensor_Value_17) AS avg_sensor_17,
    AVG(Sensor_Value_18) AS avg_sensor_18,
    AVG(Sensor_Value_19) AS avg_sensor_19,
    AVG(Sensor_Value_20) AS avg_sensor_20,
    AVG(Sensor_Value_21) AS avg_sensor_21,
    AVG(Sensor_Value_22) AS avg_sensor_22,
    AVG(Sensor_Value_23) AS avg_sensor_23,
    AVG(Sensor_Value_24) AS avg_sensor_24,
    AVG(Sensor_Value_25) AS avg_sensor_25,
    AVG(Sensor_Value_26) AS avg_sensor_26,
    AVG(Sensor_Value_27) AS avg_sensor_27,
    AVG(Sensor_Value_28) AS avg_sensor_28,
    AVG(Sensor_Value_29) AS avg_sensor_29,
    AVG(Sensor_Value_30) AS avg_sensor_30,
    AVG(Sensor_Value_31) AS avg_sensor_31,
    AVG(Sensor_Value_32) AS avg_sensor_32,
    AVG(Sensor_Value_33) AS avg_sensor_33,
    AVG(Sensor_Value_34) AS avg_sensor_34,
    AVG(Sensor_Value_35) AS avg_sensor_35,
    AVG(Sensor_Value_36) AS avg_sensor_36,
    AVG(Sensor_Value_37) AS avg_sensor_37,
    AVG(Sensor_Value_38) AS avg_sensor_38,
    AVG(Sensor_Value_39) AS avg_sensor_39,
    AVG(Sensor_Value_40) AS avg_sensor_40,
    AVG(Sensor_Value_41) AS avg_sensor_41,
    AVG(Sensor_Value_42) AS avg_sensor_42,
    AVG(Sensor_Value_43) AS avg_sensor_43,
    AVG(Sensor_Value_44) AS avg_sensor_44,
    AVG(Sensor_Value_45) AS avg_sensor_45,
    AVG(Sensor_Value_46) AS avg_sensor_46,
    AVG(Sensor_Value_47) AS avg_sensor_47,
    AVG(Sensor_Value_48) AS avg_sensor_48,
    AVG(Sensor_Value_49) AS avg_sensor_49,
    AVG(Sensor_Value_50) AS avg_sensor_50
FROM
    sensor
GROUP BY
    machine_status """)

conn.execute(view_query)

viewResult = conn.execute("select avg_sensor_50,machine_status from sensor_summary")
print(viewResult.fetchall())