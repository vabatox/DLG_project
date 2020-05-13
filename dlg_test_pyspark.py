# import necessary libraries
import pandas as pd 
import numpy
import matplotlib.pyplot as plt 
from pyspark.sql import SparkSession
from glob import glob
import os
import pyarrow.parquet as pq
import pyarrow as pa

#get all weather csv files
weather_files = sorted(glob(os.getcwd()+'/weather*.csv'))
weather_files

#join the weather files
weather_concat = pd.concat((pd.read_csv(file).assign(filename=file) for file in weather_files),ignore_index=
          True)
#check weather_concat
#weather_concat

#convert to parquet
table = pa.Table.from_pandas(weather_concat)

pq.write_table(table, 'weather.parquet')
#print(pq.read_table('weather.parquet'))

#read the parquet file
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("weather_data").getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# to read parquet file
df = sqlContext.read.parquet('weather.parquet')

#check columns & data types
#df.columns
#df.dtypes

#check count
df.createOrReplaceTempView("weather")
#sql_results = spark.sql("SELECT count(1) FROM weather limit 10")
#sql_results.show()
##194697

#Which date was the hottest day? 
##not counting null pressure as ScreenTemperature = -99
q1 = """
select
ObservationDate AS Hottest_Day
from
(
SELECT 
ObservationDate,
--sum(cast(ScreenTemperature as float))
avg(ScreenTemperature)
FROM weather
where 
Pressure is not null
group by 
ObservationDate
order by 2 desc
limit 1
)
"""

sql_results = spark.sql(q1)
#output to /tmp/hottest_day/
sql_results.coalesce(1).write.format('csv').mode("overwrite").save("/tmp/hottest_day", header='true')

sql_results.show()

#+-------------------+
#|        Hottest_Day|
#+-------------------+
#|2016-02-01T00:00:00|
#+-------------------+

#What was the temperature on that day? 

#Hottest_Day1 = sql_results.collect()
#print(Hottest_Day1)

q2 = """
SELECT 
avg(ScreenTemperature) as avg_temp_across_regions
FROM weather
where 
Pressure is not null and
ObservationDate = '2016-02-01T00:00:00'
limit 1
"""

sql_results2 = spark.sql(q2)
#output to /tmp/hottest_day_temp/
sql_results2.coalesce(1).write.format('csv').mode("overwrite").save("/tmp/hottest_day_temp", header='true')
sql_results2.show()

# In which region was the hottest day? 
q3 = """
SELECT 
Region,
ObservationDate,
avg(ScreenTemperature) as avg_temp
FROM weather
where 
Pressure is not null
group by
Region,
ObservationDate
order by 3 desc

limit 1
"""

sql_results3 = spark.sql(q3)
#output to /tmp/hottest_day_region/
sql_results3.coalesce(1).write.format('csv').mode("overwrite").save("/tmp/hottest_day_region", header='true')
sql_results3.show()