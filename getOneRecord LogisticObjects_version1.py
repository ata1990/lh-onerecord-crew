# Databricks notebook source
from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
import pyspark.sql.functions as f
from pyld import jsonld

# COMMAND ----------

# MAGIC %md
# MAGIC ###getting access token for the authorization to oneRecord Server 

# COMMAND ----------


# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the token endpoint and required parameters
token_endpoint = 'https://<onerecordserver>/b8a746a6-c540-43f4-92f3-6150ad057215/b2c_1a_hackathon/oauth2/v2.0/token'
client_id = '<clientId>'
client_secret = '<clientSecret>'
scope = '<scope>'

# Make a request to the token endpoint to obtain the bearer token
response = requests.post(
    token_endpoint,
    data={
        'grant_type': 'client_credentials',
        'scope': scope,
        'client_id': client_id,
        'client_secret': client_secret,
    }
)
# Extract the bearer token from the response
token = response.json()['access_token']

# Use the bearer token for further operations
# For example, you can pass the token as a header in subsequent requests
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/ld+json;charset=UTF-8'
}
print(headers)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Define the connection strings to access the StorageAccount

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=<SAS-token>")

# COMMAND ----------

# MAGIC %md
# MAGIC ##utility function to be called

# COMMAND ----------

def get_requests (url):
    url=url
    response=requests.get(url,headers=headers)
    df=json.loads(response.content)
    schema = json.dumps(df, indent=2)
    df_flattened=jsonld.flatten(df)
    return df_flattened

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Request to waybill logistic object

# COMMAND ----------

awb_flattened=get_requests('https://lhind.one-record.lhind.dev/logistics-objects/awb-FRA-22510353')
spark.createDataFrame(awb_flattened).write.format("parquet").mode("append").save("abfss://<container>@<storage-account>.dfs.core.windows.net/dataingestion/awbLogisticObjects")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Request to Booking logistic object

# COMMAND ----------

bkg_flattened=get_requests('https://lhind.one-record.lhind.dev/logistics-objects/booking-225')
spark.createDataFrame(bkg_flattened).write.format("parquet").mode("append").save("abfss://<container>@<storage-account>.dfs.core.windows.net/dataingestion/bookingLogisticObjects")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Request to Shipment logisticsObjects

# COMMAND ----------

shipment_flattened=get_requests('https://lhind.one-record.lhind.dev/logistics-objects/ship-2251')
spark.createDataFrame(shipment_flattened).write.format("parquet").mode("append").save("abfss://<container>@<storage-account>.dfs.core.windows.net/dataingestion/shipmentLogisticObjects")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Request to Piece logistics

# COMMAND ----------

piece_flattened=get_requests('https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-2251-1')
spark.createDataFrame(piece_flattened).write.format("parquet").mode("append").save("abfss://<container>@<storage-account>.dfs.core.windows.net/dataingestion/pieceLogisticObjects")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Request to Shipment logisticsEvents

# COMMAND ----------

shipmentEvents_flattened=get_requests('https://lhind.one-record.lhind.dev/logistics-objects/ship-2251/logistics-events')
spark.createDataFrame(shipmentEvents_flattened).write.format("parquet").mode("append").save("abfss://<container>@<storage-account>.dfs.core.windows.net/dataingestion/shipmentLogisticEvents")
