# Databricks notebook source
# MAGIC %md
# MAGIC ####Define the connection strings to access the StorageAccount

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=<SAS-token>")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Import Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import row_number,lit

# COMMAND ----------

# MAGIC %md
# MAGIC ####Steps:
# MAGIC -Data provided stored in datalake
# MAGIC -Read them 
# MAGIC -Further etl processing

# COMMAND ----------

 dd40_df= (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load(
        "abfss://<container>@<storage-account>.dfs.core.windows.net/cargoIQ/DD40-SCH-2023-01-sample.csv"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####enriching the data with Planned EventDate

# COMMAND ----------

distinctAWBNo = dd40_df.select("HAWB_No").distinct()
w = Window().orderBy(lit('A'))
rowNoDf = distinctAWBNo.withColumn("row_num", row_number().over(w))

# COMMAND ----------

#map the rowNumber information with the main dataset
dd40_df=dd40_df.join(rowNoDf,on="HAWB_No", how="inner")

# COMMAND ----------


# Register the DataFrame as a temporary view
dd40_df.createOrReplaceTempView("vdd40_df")

# Execute the SQL query
generateDLV_df = spark.sql("""
    SELECT
        HAWB_No AS awbNo,
        HAWB_Origin AS departureLocation,
        HAWB_Destination AS arrivalLocation,
        Baseline_Pieces AS total_no_of_pieces,
        Baseline_Weight AS total_weight,
        Baseline_Volume AS total_net_volume,
        'DLV' AS eventCode,
        baseline_date AS planEventDate,
        CASE
            WHEN actual_date IS NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 1 AND 1000
                THEN timestampadd(MINUTE, 60, dynamic_date)
            WHEN actual_date IS NOT NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 1 AND 1000
                THEN timestampadd(MINUTE, 30, actual_date)
            WHEN actual_date IS NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 1001 AND 2000
                THEN timestampadd(MINUTE, 120, dynamic_date)
            WHEN actual_date IS NOT NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 1001 AND 2000
                THEN timestampadd(MINUTE, 50, actual_date)
            WHEN actual_date IS NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 2001 AND 3000
                THEN timestampadd(MINUTE, 20, dynamic_date)
            WHEN actual_date IS NOT NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 2001 AND 3000
                THEN timestampadd(MINUTE, 45, actual_date)
            WHEN actual_date IS NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 3001 AND 4000
                THEN timestampadd(MINUTE, 40, dynamic_date)
            WHEN actual_date IS NOT NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 3001 AND 4000
                THEN timestampadd(MINUTE, 30, actual_date)
            WHEN actual_date IS NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 4001 AND 5000
                THEN timestampadd(MINUTE, 90, dynamic_date)
            WHEN actual_date IS NOT NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 4001 AND 5000
                THEN timestampadd(MINUTE, 180, actual_date)
            WHEN actual_date IS NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 5001 AND 6000
                THEN timestampadd(MINUTE, 65, dynamic_date)
            WHEN actual_date IS NOT NULL AND Milestone LIKE 'RIH' AND row_num BETWEEN 5001 AND 6000
                THEN timestampadd(MINUTE, 80, actual_date)
            ELSE actual_date
        END AS eventDate
    FROM (
        SELECT
            row_num,
            Month,
            Forwarder,
            CDMPF,
            HAWB_No,
            HAWB_Origin,
            HAWB_Destination,
            Pickup_condition,
            Delivery_Condition,
            Milestone_Location,
            Partial_Indicator,
            Baseline_Pieces,
            Actual_Pieces,
            Baseline_Weight,
            Actual_Weight,
            Weight_Indicator,
            Baseline_Volume,
            Actual_Volume,
            Volume_Indicator,
            Milestone,
            Baselined,
            Dynamic,
            actual,
            CASE
                WHEN LENGTH(Baselined) = 14 THEN to_timestamp(Baselined, 'dd-MM-yy HH:mm')
                ELSE to_timestamp(Baselined, 'dd-MM-yy H:mm')
            END AS baseline_date,
            CASE
                WHEN LENGTH(Dynamic) = 14 THEN to_timestamp(Dynamic, 'dd-MM-yy HH:mm')
                ELSE to_timestamp(Dynamic, 'dd-MM-yy H:mm')
            END AS dynamic_date,
            CASE
                WHEN LENGTH(Actual) = 14 THEN to_timestamp(Actual, 'dd-MM-yy HH:mm')
                ELSE to_timestamp(Actual, 'dd-MM-yy H:mm')
            END AS actual_date
        FROM vdd40_df
        WHERE Milestone LIKE 'RIH'
    )
""")


# COMMAND ----------

# Register the DataFrame as a temporary view
generateDLV_df.createOrReplaceTempView("vgenerateDLV_df")

# Execute the SQL query
cargoIQevent_df = spark.sql("""select HAWB_No as awbNo ,
  HAWB_Origin as departureLocation,
  HAWB_Destination as arrrivalLocation,
  Baseline_Pieces as total_no_of_pieces,
  Baseline_Weight as total_weight,
  Baseline_Volume as total_net_volume,
  Milestone as  eventCode,
case when len(Baselined) = 14 then to_timestamp(Baselined, "dd-MM-yy HH:mm") else to_timestamp(Baselined, "dd-MM-yy H:mm") end planEventDate,
case when len(Actual) = 14 then to_timestamp(Actual, "dd-MM-yy HH:mm") else to_timestamp(Actual, "dd-MM-yy H:mm") end eventDate from
      vdd40_df where actual is not null""")

# COMMAND ----------

#union DLV Events with all CargoIQ events
allEvent_df=cargoIQevent_df.union(generateDLV_df)

# COMMAND ----------

# Register the DataFrame as a temporary view
allEvent_df.createOrReplaceTempView('vallEvent_df')
# Execute the SQL query
allEvent_df = spark.sql("""select awbNo,
departureLocation,
arrrivalLocation,
total_no_of_pieces,
total_weight,
total_net_volume,greatest(cast(total_weight as double) ,((cast(total_net_volume as double)*1000)/6)) as wgt_chargeable,
eventCode,
planEventDate,
eventDate from vallEvent_df where eventCode in ('DLV','RIH',
'DIH',
'RIW',
'OFD,'
'TPN',
'POD')""")

# COMMAND ----------

#write the final dataframe in Azure DataLake
allEvent_df.write.format("parquet").mode("overwrite").save("abfss://<container>@<storage-account>.dfs.core.windows.net/finalData/cargoIQEvent")

# COMMAND ----------

allEvent_df.write.format("csv").mode("overwrite").save("abfss://<container>@<storage-account>.dfs.core.windows.net/finalData_csv/cargoIQEvent")
