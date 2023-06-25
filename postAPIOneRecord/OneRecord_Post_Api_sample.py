# Databricks notebook source
spark.sql("set spart.databricks.delta.preview.enabled=true")

spark.sql("set spart.databricks.delta.retentionDutationCheck.preview.enabled=false")

# COMMAND ----------

from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
import pyspark.sql.functions as f
from pyld import jsonld
from requests.auth import HTTPDigestAuth

# COMMAND ----------

# MAGIC %md
# MAGIC ###Getting access token for the authorization to oneRecord Server 

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
# MAGIC ###Read Cargo DataIQ sample 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=<SAS-token>")

# COMMAND ----------

cargoIQSample = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load(
        "abfss://<container>@<storage-account>.dfs.core.windows.net/cargoIQ/DD40-SCH-2023-01-sample.csv"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##post one CargoIQ sample 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### POST WAYBILL

# COMMAND ----------

import requests

json_payload = """{
  "@id": "https://lhind.one-record.lhind.dev/logistics-objects/awb-FRA-22510455",
  "@type": "https://onerecord.iata.org/ns/cargo#Waybill",
  "https://onerecord.iata.org/ns/cargo#arrivalLocation": {
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/fra"
  },
  "https://onerecord.iata.org/ns/cargo#departureLocation": {
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/jfk"
  },
  "https://onerecord.iata.org/ns/cargo#involvedParties": [
    {
      "@id": "neone:6048bc74-b229-4213-8042-94fbe94d8de2",
      "@type": "https://onerecord.iata.org/ns/cargo#Party",
      "https://onerecord.iata.org/ns/cargo#organization": {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/organization-shp"
      },
      "https://onerecord.iata.org/ns/cargo#role": "Shipper"
    },
    {
      "@id": "neone:7316ca38-09bf-47b6-917f-c3aec4c7dac4",
      "@type": "https://onerecord.iata.org/ns/cargo#Party",
      "https://onerecord.iata.org/ns/cargo#organization": {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/organization-cne"
      },
      "https://onerecord.iata.org/ns/cargo#role": "Consignee"
    },
    {
      "@id": "neone:efd376a0-11b2-4e99-9730-0aa4939ba8ef",
      "@type": "https://onerecord.iata.org/ns/cargo#Party",
      "https://onerecord.iata.org/ns/cargo#organization": {
        "@id": "https://lhind.one-record.lhind.dev/logistics-objects/company-fwd"
      },
      "https://onerecord.iata.org/ns/cargo#role": "FreightForwarder"
    }
  ],
  "https://onerecord.iata.org/ns/cargo#referredBookingOption": {
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/booking-225"
  },
  "https://onerecord.iata.org/ns/cargo#shipment": {
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"
  },
  "https://onerecord.iata.org/ns/cargo#waybillPrefix": "FRA",
  "https://onerecord.iata.org/ns/cargo#waybillType": "Master",
  "https://onerecord.iata.org/ns/cargo#waybillnumber": "22510455"
}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects', data=json_payload, headers=headers)

print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ### POST BOOKING
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC import requests
# MAGIC
# MAGIC json_payload = """{
# MAGIC   "@id": "https://lhind.one-record.lhind.dev/logistics-objects/booking-225",
# MAGIC   "@type": "https://onerecord.iata.org/ns/cargo#Booking",
# MAGIC   "https://onerecord.iata.org/ns/cargo#activitySequences": [
# MAGIC     {
# MAGIC       "@id": "neone:71960f61-6e66-47fc-a7ab-b9fe2e96f812",
# MAGIC       "@type": "https://onerecord.iata.org/ns/cargo#ActivitySequence",
# MAGIC       "https://onerecord.iata.org/ns/cargo#activity": {
# MAGIC         "@id": "https://lhind.one-record.lhind.dev/logistics-objects/transport-movement-1"
# MAGIC       },
# MAGIC       "https://onerecord.iata.org/ns/cargo#sequenceNumber": "1"
# MAGIC     },
# MAGIC     {
# MAGIC       "@id": "neone:fd5db5b9-1944-43fa-826d-4b5b4e84f729",
# MAGIC       "@type": "https://onerecord.iata.org/ns/cargo#ActivitySequence",
# MAGIC       "https://onerecord.iata.org/ns/cargo#activity": {
# MAGIC         "@id": "https://lhind.one-record.lhind.dev/logistics-objects/transport-movement-2"
# MAGIC       },
# MAGIC       "https://onerecord.iata.org/ns/cargo#sequenceNumber": "2"
# MAGIC     }
# MAGIC   ],
# MAGIC   "https://onerecord.iata.org/ns/cargo#issuedForWaybill": {
# MAGIC     "@id": "https://lhind.one-record.lhind.dev/logistics-objects/awb-FRA-22510455"
# MAGIC   }
# MAGIC }"""
# MAGIC
# MAGIC r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects', data=json_payload, headers=headers)
# MAGIC
# MAGIC print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ### POST SHIPPMENT 

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

print(r.headers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### POST PIECES 
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

import requests

json_payload = """{
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-2",
    "@type": "https://onerecord.iata.org/ns/cargo#Piece",
    "https://onerecord.iata.org/ns/cargo#goodsDescription": "General Cargo",
    "https://onerecord.iata.org/ns/cargo#grossWeight": {
        "@id": "neone:76acc73d-f3c8-484f-91ad-724d25d0963c",
        "@type": "https://onerecord.iata.org/ns/cargo#Value",
        "https://onerecord.iata.org/ns/cargo#numericalValue": {
            "@type": "http://www.w3.org/2001/XMLSchema#double",
            "@value": "10.0"
        },
        "https://onerecord.iata.org/ns/cargo#unit": "KGM"
    }
}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

import requests

json_payload = """{
    "@id": "https://lhind.one-record.lhind.dev/logistics-objects/piece-ship-22510-3",
    "@type": "https://onerecord.iata.org/ns/cargo#Piece",
    "https://onerecord.iata.org/ns/cargo#goodsDescription": "General Cargo",
    "https://onerecord.iata.org/ns/cargo#grossWeight": {
        "@id": "neone:76acc73d-f3c8-484f-91ad-724d25d0963c",
        "@type": "https://onerecord.iata.org/ns/cargo#Value",
        "https://onerecord.iata.org/ns/cargo#numericalValue": {
            "@type": "http://www.w3.org/2001/XMLSchema#double",
            "@value": "23.0"
        },
        "https://onerecord.iata.org/ns/cargo#unit": "KGM"
    }
}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

print(r.headers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### POST LOGISTIC EVENTS -SHIPMENT

# COMMAND ----------

## logistinc object ship-22510

import requests

json_payload = """{

    "@type":"https://onerecord.iata.org/ns/cargo#LogisticsEvent",

    "https://onerecord.iata.org/ns/cargo#eventCode":"FOH",

    "https://onerecord.iata.org/ns/cargo#eventDate":{

        "@type":"http://www.w3.org/2001/XMLSchema#dateTime",

        "@value":"2023-05-18T18:03:00Z"

    },

    "https://onerecord.iata.org/ns/cargo#eventName":"Freight On Hand",

    "https://onerecord.iata.org/ns/cargo#eventTimeType":"Actual",

    "https://onerecord.iata.org/ns/cargo#linkedObject":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-22510/logistics-events', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

## logistinc object ship-22510

import requests

json_payload = """{

    "@type":"https://onerecord.iata.org/ns/cargo#LogisticsEvent",

    "https://onerecord.iata.org/ns/cargo#eventCode":"DIS",

    "https://onerecord.iata.org/ns/cargo#eventDate":{

        "@type":"http://www.w3.org/2001/XMLSchema#dateTime",

        "@value":"2023-05-19T16:00:00Z"

    },

    "https://onerecord.iata.org/ns/cargo#eventName":"Irregularity",

    "https://onerecord.iata.org/ns/cargo#eventTimeType":"Actual",

    "https://onerecord.iata.org/ns/cargo#linkedObject":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-22510/logistics-events', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

## logistinc object ship-22510

import requests

json_payload = """{

    "@type":"https://onerecord.iata.org/ns/cargo#LogisticsEvent",

    "https://onerecord.iata.org/ns/cargo#eventCode":"DEP",

    "https://onerecord.iata.org/ns/cargo#eventDate":{

        "@type":"http://www.w3.org/2001/XMLSchema#dateTime",

        "@value":"2023-05-19T18:15:00Z"

    },

    "https://onerecord.iata.org/ns/cargo#eventName":"Departed",

    "https://onerecord.iata.org/ns/cargo#eventTimeType":"Actual",

    "https://onerecord.iata.org/ns/cargo#linkedObject":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-22510/logistics-events', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

## logistinc object ship-22510

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

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-22510/logistics-events', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

## logistinc object ship-22510

import requests

json_payload = """{

    "@type":"https://onerecord.iata.org/ns/cargo#LogisticsEvent",

    "https://onerecord.iata.org/ns/cargo#eventCode":"ARR",

    "https://onerecord.iata.org/ns/cargo#eventDate":{

        "@type":"http://www.w3.org/2001/XMLSchema#dateTime",

        "@value":"2023-05-19T20:55:00Z"

    },

    "https://onerecord.iata.org/ns/cargo#eventName":"Arrived",

    "https://onerecord.iata.org/ns/cargo#eventTimeType":"Predicted",
    "https://onerecord.iata.org/ns/cargo#irregularityProbability": {

    "@type": "http://www.w3.org/2001/XMLSchema#double",

    "@value": "0.39"
    },
    "https://onerecord.iata.org/ns/cargo#linkedObject":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-22510/logistics-events', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

## logistinc object ship-22510

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

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-22510/logistics-events', data=json_payload, headers=headers)
print(r)

# COMMAND ----------

## logistinc object ship-22510

import requests

json_payload = """{

    "@type":"https://onerecord.iata.org/ns/cargo#LogisticsEvent",

    "https://onerecord.iata.org/ns/cargo#eventCode":"RCS",

    "https://onerecord.iata.org/ns/cargo#eventDate":{

        "@type":"http://www.w3.org/2001/XMLSchema#dateTime",

        "@value":"2023-05-19T00:36:00Z"

    },

    "https://onerecord.iata.org/ns/cargo#eventName":"Ready for Carriage",

    "https://onerecord.iata.org/ns/cargo#eventTimeType":"Predicted",
    "https://onerecord.iata.org/ns/cargo#irregularityProbability": {

    "@type": "http://www.w3.org/2001/XMLSchema#double",

    "@value": "0.39"
    },
    "https://onerecord.iata.org/ns/cargo#linkedObject":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/ship-22510"

    },

    "https://onerecord.iata.org/ns/cargo#recordedAtLocation":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/qsh"

    },

    "https://onerecord.iata.org/ns/cargo#recordedBy":{

    "@id":"https://lhind.one-record.lhind.dev/logistics-objects/carrier-lcag"

    }

}"""

r = requests.post('https://lhind.one-record.lhind.dev/logistics-objects/ship-22510/logistics-events', data=json_payload, headers=headers)
print(r)
