# Databricks notebook source
spark.sql("set spart.databricks.delta.preview.enabled=true")

spark.sql("set spart.databricks.delta.retentionDutationCheck.preview.enabled=false")

# COMMAND ----------

# MAGIC %md
# MAGIC ###import  libraries 

# COMMAND ----------

from pyspark.sql import SparkSession
import requests
import json
import uuid
import pandas as pd
import pyspark.sql.functions as f
from pyld import jsonld
from requests.auth import HTTPDigestAuth

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
# MAGIC ###Read model output from data lake to post reslts into oneRecord Server 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=<SAS-token>")

# COMMAND ----------


# Execute the SQL query select required attributes to be posted

df = spark.sql(""" select * from default.output""")
result_df=df.select("awbNo","vol_gross",
"departureLocation",
"arrrivalLocation",
"total_no_of_pieces",
"total_weight",
"total_net_volume",
"delay",
"delay_code",
"eventDate",
"event_occurence_date_time_utc",
"discrepancy_code",
"irreg_reason_code",
"irreg_desc",
"eventCode",
"irregProbability")

# COMMAND ----------

display(result_df)

# COMMAND ----------

display(result_df.filter("awbNo='FRA-32647160'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Post Request Shipment 

# COMMAND ----------

shipment_df=result_df.select( "awbNo","total_weight","total_no_of_pieces") \
                      .groupBy(["awbNo"]) \
.agg(f.sum("total_weight").alias("total_weight"),f.count("total_no_of_pieces").alias("no_of_pieces"))

# COMMAND ----------

shipment_df = shipment_df.withColumn('waybill', f.expr("uuid()"))

# COMMAND ----------

display(shipment_df.select(f.col("awbNo")))

# COMMAND ----------


for awb in shipment_df.collect().
    #json string
    print(awb.select(f.col("waybill"))) 

# COMMAND ----------

import requests

json_payload = """{
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/"+shipment_df.select(f.col("waybill")),
    "@type": "https://onerecord.iata.org/ns/cargo#Shipment",
    "https://onerecord.iata.org/ns/cargo#goodsDescription": "General Cargo",
    "https://onerecord.iata.org/ns/cargo#shipmentOfPieces": [ {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-1"
    },
    {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-2"
    },
    {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-2"
    }],
    "https://onerecord.iata.org/ns/cargo#waybill":{
        "@id":"https://lhind.one-record.lhind.dev/logistics-objects/awb-FRA-22510455"
    }
}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects', data=json_payload, headers=headers)

print(r)

# COMMAND ----------

pieces_df = shipment_df.withColumn('waybill', f.expr("uuid()"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utility function to create json objects
# MAGIC

# COMMAND ----------

results = results_df.toJSON().map(json.loads).collect()
print(results)

# COMMAND ----------

import json
# toJSON() turns each row of the DataFrame into a JSON string
# calling first() on the result will fetch the first row.

results = json.loads(result.toJSON().first())
for key in results:
    print(results[key])

# To decode the entire DataFrame iterate over the result
# of toJSON()

def print_rows(row):
    data = json.loads(row)
    for key in data:
        print("{key}:{value}".format(key=key, value=data[key]))


results = result.toJSON()
results.foreach(print_rows) 

# COMMAND ----------



# COMMAND ----------

import requests

json_payload = """{
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/ship-22510",
    "@type": "https://onerecord.iata.org/ns/cargo#Shipment",
    "https://onerecord.iata.org/ns/cargo#goodsDescription": "General Cargo",
    "https://onerecord.iata.org/ns/cargo#shipmentOfPieces": [ {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-1"
    },
    {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-2"
    },
    {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-2"
    }],
    "https://onerecord.iata.org/ns/cargo#waybill":{
        "@id":"https://lhind.one-record.lhind.dev/logistics-objects/awb-FRA-22510455"
    }
}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects', data=json_payload, headers=headers)

print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post Request Pieces 
# MAGIC

# COMMAND ----------

import requests

json_payload = """{
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-1",
    "@type": "https://onerecord.iata.org/ns/cargo#Piece",
    "https://onerecord.iata.org/ns/cargo#goodsDescription": "General Cargo",
    "https://onerecord.iata.org/ns/cargo#grossWeight": {
        "@id": "neone:76acc73d-f3c8-484f-91ad-724d25d0963c",
        "@type": "https://onerecord.iata.org/ns/cargo#Value",
        "https://onerecord.iata.org/ns/cargo#numericalValue": {
            "@type": "http://www.w3.org/2001/XMLSchema#double",
            "@value": "30.0"
        },
        "https://onerecord.iata.org/ns/cargo#unit": "KGM"
    }
}"""


r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post Request Logicstic Events

# COMMAND ----------

## logistinc object shipment 5

import requests

json_payload = """{

    "@type":"https://onerecord.iata.org/ns/cargo#LogisticsEvent",

    "https://onerecord.iata.org/ns/cargo#eventCode":"FOW",

    "https://onerecord.iata.org/ns/cargo#eventDate":{

        "@type":"http://www.w3.org/2001/XMLSchema#dateTime",

        "@value":"2023-05-19T20:04:00Z"

    },

    "https://onerecord.iata.org/ns/cargo#eventName":"Freight out Warehouse",

    "https://onerecord.iata.org/ns/cargo#eventTimeType":"Predicted",
    "https://onerecord.iata.org/ns/cargo#irregularityProbability": {

    "@type": "http://www.w3.org/2001/XMLSchema#double",

    "@value": "0.78"
    },
    "https://onerecord.iata.org/ns/cargo#linkedObject":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-2251"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-2251/logistics-events', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

## logistinc object shipment 5

import requests

json_payload = """{

    "@type":"https://onerecord.iata.org/ns/cargo#LogisticsEvent",

    "https://onerecord.iata.org/ns/cargo#eventCode":"RCF",

    "https://onerecord.iata.org/ns/cargo#eventDate":{

        "@type":"http://www.w3.org/2001/XMLSchema#dateTime",

        "@value":"2023-05-19T22:54:00Z"

    },

    "https://onerecord.iata.org/ns/cargo#eventName":"Received Cargo from Flight",

    "https://onerecord.iata.org/ns/cargo#eventTimeType":"Predicted",
    "https://onerecord.iata.org/ns/cargo#irregularityProbability": {

    "@type": "http://www.w3.org/2001/XMLSchema#double",

    "@value": "0.39"
    },
    "https://onerecord.iata.org/ns/cargo#linkedObject":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-2251"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-2251/logistics-events', data=json_payload, headers=headers)
print(r)
