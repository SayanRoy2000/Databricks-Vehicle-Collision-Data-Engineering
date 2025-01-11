# Databricks notebook source
# MAGIC %md
# MAGIC #Motor_Vehicle_Collisions_Crashes

# COMMAND ----------

# MAGIC %pip install requests
# MAGIC %pip install folium

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

import requests
api_url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
response = requests.get(api_url)
if response.status_code == 200:
    data = response.json()
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

# COMMAND ----------

crashDF=spark.createDataFrame(data)
crashDF.printSchema()
display(crashDF)

# COMMAND ----------
#If you have the dataset file then you can read it directly for better result
#file_location="/FileStore/tables/unzipped_file.csv"
#crashDF=spark.read.format("csv").option("header","true").option("inferSchema","true").load(file_location)
#display(crashDF)

# COMMAND ----------

filterDF=crashDF.dropDuplicates(["COLLISION_ID"]).filter((crashDF.COLLISION_ID.isNotNull())|(col("CRASH DATE").isNotNull())|(col("CRASH TIME").isNotNull()))
display(filterDF)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
filterDF = filterDF.withColumn("CRASH DATE", date_format(to_timestamp(trim(col("CRASH DATE")),"MM/dd/yyyy"), "yyyy-MM-dd"))
filterDF = filterDF.withColumn("CRASH TIME", date_format(to_timestamp(trim(col("CRASH TIME")), "HH:mm"), "HH:mm:ss"))
display(filterDF)

# COMMAND ----------

filterDF=filterDF.withColumn("crash_timestamp",to_timestamp(concat(col("CRASH DATE"),lit(" "),col("CRASH TIME")),"yyyy-MM-dd HH:mm:ss"))
filterDF=filterDF.withColumn("year", year(filterDF["crash_timestamp"])) \
       .withColumn("month", month(filterDF["crash_timestamp"])) \
       .withColumn("day", dayofmonth(filterDF["crash_timestamp"])) \
       .withColumn("day_of_week",date_format(filterDF["crash_timestamp"],"EEEE"))
display(filterDF)

# COMMAND ----------

zipDF=filterDF.groupBy("ZIP CODE").agg(count("*").alias("Crash_count"),avg("NUMBER OF PERSONS INJURED").alias("Avg_injured"))
display(zipDF)

# COMMAND ----------

factorDF=filterDF.groupBy(col("CONTRIBUTING FACTOR VEHICLE 1")).count()
factorDF=factorDF.filter((col("CONTRIBUTING FACTOR VEHICLE 1")!="Unspecified")&(col("CONTRIBUTING FACTOR VEHICLE 1").isNotNull())&(~col("CONTRIBUTING FACTOR VEHICLE 1").rlike("^[0-9]+$")))
factorDF=factorDF.orderBy(col("count").desc()).select("CONTRIBUTING FACTOR VEHICLE 1").limit(5)
factoryDF=factorDF.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1","Top Contributing Factor For Crashes")
display(factorDF)

# COMMAND ----------

hotspotDF=filterDF.groupBy(col("LOCATION"),col("LATITUDE"),col("LONGITUDE")).count()
hotspotDF = hotspotDF.orderBy(col("count").desc()).filter((col("LATITUDE").isNotNull())&(col("LONGITUDE").isNotNull())&(col("LATITUDE") != 0)&(col("LONGITUDE") != 0)).limit(100)
display(hotspotDF)

# COMMAND ----------

avgDF=hotspotDF.agg(avg(col("LATITUDE")),avg(col("LONGITUDE")))
display(avgDF)

# COMMAND ----------

import folium
hotspots_pd = hotspotDF.toPandas()
m = folium.Map(location=[40.732827284999985, -73.921942248], zoom_start=12)
for _, row in hotspots_pd.iterrows():
    folium.CircleMarker(
        location=[row["LATITUDE"], row["LONGITUDE"]],
        radius=row["count"] / 10,
        color='red',
        fill=True,
        fill_color='red'
    ).add_to(m)
displayHTML(m._repr_html_())


# COMMAND ----------

yearlyDF=filterDF.groupBy("year").count()
yearlyDF=yearlyDF.withColumnRenamed("count","Total_Crashes").orderBy("year")
display(yearlyDF)

# COMMAND ----------

monthlyDF=filterDF.withColumn("Month_name",date_format("CRASH DATE","MMMM"))
monthlyDF=monthlyDF.groupBy("Month_name","month","year").count().orderBy("year","month").drop("month")
monthlyDF=monthlyDF.withColumnRenamed("count","Total_Crashes")
monthlyDF=monthlyDF.withColumnRenamed("Month_name","month")
display(monthlyDF)

# COMMAND ----------

dailyDF=filterDF.withColumn("Month_name",date_format("CRASH DATE","MMMM"))
dailyDF=dailyDF.groupBy("day","Month_name","month","year").count().orderBy("year","month","day").drop("month")
dailyDF=dailyDF.withColumnRenamed("count","Total_Crashes")
dailyDF=dailyDF.withColumnRenamed("Month_name","month")
display(dailyDF)

# COMMAND ----------

fatalityDF=filterDF.select("NUMBER OF PEDESTRIANS INJURED","NUMBER OF PEDESTRIANS KILLED","NUMBER OF CYCLIST INJURED","NUMBER OF CYCLIST KILLED","NUMBER OF MOTORIST INJURED","NUMBER OF MOTORIST KILLED")
columns=fatalityDF.columns
sums=fatalityDF.agg(*[sum(column).alias(column) for column in columns]).collect()[0]
fatalityDF=spark.createDataFrame([sums])
display(fatalityDF)

# COMMAND ----------

pedestrianDF = fatalityDF.selectExpr(
    "'pedestrians' as victim_type",
    "`NUMBER OF PEDESTRIANS INJURED` as injured_count",
    "`NUMBER OF PEDESTRIANS KILLED` as killed_count"
)
cyclistsDF = fatalityDF.selectExpr(
    "'cyclists' as victim_type",
    "`NUMBER OF CYCLIST INJURED` as injured_count",
    "`NUMBER OF CYCLIST KILLED` as killed_count"
)
motoristsDF = fatalityDF.selectExpr(
    "'motorists' as victim_type",
    "`NUMBER OF MOTORIST INJURED` as injured_count",
    "`NUMBER OF MOTORIST KILLED` as killed_count"
)
mergedDF= pedestrianDF.union(cyclistsDF).union(motoristsDF)
display(mergedDF)

# COMMAND ----------

filterDF=filterDF.withColumn("NUMBER OF PERSONS INJURED",col("NUMBER OF PERSONS INJURED").cast(IntegerType()))
severityDF=filterDF.groupBy("CONTRIBUTING FACTOR VEHICLE 1").agg(sum("NUMBER OF PERSONS KILLED").alias("Total_Killed"),sum("NUMBER OF PERSONS INJURED").alias("Total_Injured"))
severityDF=severityDF.withColumn("Severity(in %)",col("Total_Killed")/col("Total_Injured")*100)
is_number_udf = udf(lambda value: True if value.replace('.', '', 1).isdigit() else False, BooleanType())
severityDF = severityDF.filter(
    col("CONTRIBUTING FACTOR VEHICLE 1").isNotNull() &
    (col("CONTRIBUTING FACTOR VEHICLE 1") != "Unspecified")& ~is_number_udf(col("CONTRIBUTING FACTOR VEHICLE 1"))
)
severityDF=severityDF.orderBy(col("Severity(in %)").desc())
display(severityDF)

# COMMAND ----------

vehicle_codeDF=filterDF.select("VEHICLE TYPE CODE 1","VEHICLE TYPE CODE 2","VEHICLE TYPE CODE 3","VEHICLE TYPE CODE 4")
display(vehicle_codeDF)

# COMMAND ----------

vehicle_codeDF1=vehicle_codeDF.select("VEHICLE TYPE CODE 1").dropDuplicates(["VEHICLE TYPE CODE 1"])
display(vehicle_codeDF1)
vehicle_codeDF2=vehicle_codeDF.select("VEHICLE TYPE CODE 2").dropDuplicates(["VEHICLE TYPE CODE 2"])
display(vehicle_codeDF2)
vehicle_codeDF3=vehicle_codeDF.select("VEHICLE TYPE CODE 3").dropDuplicates(["VEHICLE TYPE CODE 3"])
display(vehicle_codeDF3)
vehicle_codeDF4=vehicle_codeDF.select("VEHICLE TYPE CODE 4").dropDuplicates(["VEHICLE TYPE CODE 4"])
display(vehicle_codeDF4)

# COMMAND ----------

from pyspark.sql.types import DoubleType
import math
city_center_lat = 40.7128
city_center_lon = -74.0060

def haversine(lat1, lon1, lat2, lon2):
    R = 6371 
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

haversine_udf = udf(lambda lat, lon: haversine(lat, lon, city_center_lat, city_center_lon), DoubleType())

df = hotspotDF.withColumn('Distance_to_Center', haversine_udf(col('LATITUDE'), col('LONGITUDE')))

df=df.select('LATITUDE', 'LONGITUDE', 'Distance_to_Center')
display(df)

# COMMAND ----------

filterDF=filterDF.withColumn("Full_address",concat(trim(col("ON STREET NAME")),lit(" & "),trim(col("CROSS STREET NAME")))).drop("ON STREET NAME","CROSS STREET NAME")
display(filterDF)

# COMMAND ----------

from pyspark.sql.window import Window
zipDF=filterDF.select("BOROUGH","ZIP CODE")
windowSpec=Window.partitionBy("BOROUGH").orderBy(col("count").desc())
zipDF=zipDF.groupBy("BOROUGH","ZIP CODE").agg(count("ZIP CODE").alias("count"))
zipDF=zipDF.withColumn("rank", rank().over(windowSpec))
zipDF=zipDF.filter((col("rank") == 1)&(col("ZIP CODE").isNotNull())).drop("rank","count")
result_dict = dict(zipDF.rdd.map(lambda row: (row["BOROUGH"], row["ZIP CODE"])).collect())
get_zip = lambda k: result_dict.get(k, "Key not found")
get_zip_udf = udf(get_zip, IntegerType())
filterDF=filterDF.withColumn("ZIP CODE",when(col("ZIP CODE").isNull(), get_zip_udf(col("BOROUGH")))
                               .otherwise(col("ZIP CODE"))).filter(col("BOROUGH").isNotNull())
display(filterDF)

# COMMAND ----------

zipDF=filterDF.select("BOROUGH","ZIP CODE")
windowSpec=Window.partitionBy("BOROUGH").orderBy(col("count").desc())
zipDF=zipDF.groupBy("BOROUGH","ZIP CODE").agg(count("ZIP CODE").alias("count"))
zipDF=zipDF.withColumn("rank", rank().over(windowSpec))
zipnaDF=zipDF.filter((col("rank") == 1)&(col("ZIP CODE").isNull())).drop("rank","count","ZIP CODE")
zipnaDF=zipDF.filter((col("rank") == 2)).drop("rank","count").join(zipnaDF,on="BOROUGH",how="inner")
display(zipnaDF)
zipDF=zipDF.filter((col("rank") == 1)&(col("ZIP CODE").isNotNull())).drop("rank","count")
zipDF=zipDF.union(zipnaDF)
result_dict = dict(zipDF.rdd.map(lambda row: (row["BOROUGH"], row["ZIP CODE"])).collect())
get_zip = lambda k: result_dict.get(k, "Key not found")
get_zip_udf = udf(get_zip, IntegerType())
filterDF=filterDF.withColumn("ZIP CODE",when(col("ZIP CODE").isNull(), get_zip_udf(col("BOROUGH")))
                               .otherwise(col("ZIP CODE"))).filter(col("BOROUGH").isNotNull())
display(filterDF)

# COMMAND ----------

filterDF = filterDF.withColumn("CRASH TIME", to_timestamp(filterDF["CRASH TIME"], "HH:mm:ss"))
filterDF = filterDF.withColumn("CRASH TIME", date_format(filterDF["CRASH TIME"], "HH:mm:ss"))
timeDF=filterDF.withColumn("Time_of_day",
    when((col("CRASH TIME").between("06:00:00", "11:59:59")), "Morning")
    .when((col("CRASH TIME").between("12:00:00", "17:59:59")), "Afternoon")
    .when((col("CRASH TIME").between("18:00:00", "20:59:59")), "Evening")
    .otherwise("Night")
)
display(timeDF)

# COMMAND ----------

filterDF=filterDF.fillna("Unspecified",subset=['CONTRIBUTING FACTOR VEHICLE 1','CONTRIBUTING FACTOR VEHICLE 2','CONTRIBUTING FACTOR VEHICLE 3','CONTRIBUTING FACTOR VEHICLE 4','CONTRIBUTING FACTOR VEHICLE 5'])
display(filterDF)

# COMMAND ----------

weekendDF=filterDF.filter((col("day_of_week")=="Sunday")|(col("day_of_week")=="Saturday"))
display(weekendDF)

# COMMAND ----------

windowspecs=Window.partitionBy("BOROUGH").orderBy(col("NUMBER OF PERSONS INJURED").desc())
boroughDF=filterDF.select("COLLISION_ID","BOROUGH","NUMBER OF PERSONS INJURED")
boroughDF=boroughDF.withColumn("Rank",rank().over(windowspecs))
boroughDF=boroughDF.withColumn("row_number", row_number().over(windowspecs))
boroughDF=boroughDF.filter(col("row_number") <= 1000).drop("row_number").drop("row_number")
display(boroughDF)

# COMMAND ----------

collision_countDF = filterDF.groupBy(col('CRASH DATE')).agg(count("*").alias("crash_count"))
collision_countDF = collision_countDF.withColumn("CRASH DATE", to_date(col("CRASH DATE"), "yyyy-MM-dd"))
window_spec = Window.orderBy("CRASH DATE").rowsBetween(-6, 0)
collision_countDF = collision_countDF.withColumn("rolling_avg", round(avg(col("crash_count")).over(window_spec),2))
collision_countDF = collision_countDF.select("CRASH DATE", "crash_count", "rolling_avg")
display(collision_countDF)
