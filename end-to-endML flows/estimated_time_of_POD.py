# Databricks notebook source
# MAGIC %md
# MAGIC #POD prediction (duration between DLV and POD)
# MAGIC ### This notebook contains data preprocessing, feature engineering and model training for prediction of (duration between DLV and POD)

# COMMAND ----------

# MAGIC %md
# MAGIC #Import the libraries 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import dayofweek, hour, month, year, weekofyear, dayofmonth
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, lead, coalesce
from pyspark.sql.functions import expr
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from databricks.automl_runtime.sklearn import OneHotEncoder
from xgboost import XGBRegressor
import pandas as pd
import numpy as np
import mlflow
from mlflow.models import Model, infer_signature, ModelSignature
from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
import sklearn
from sklearn import set_config
from hyperopt import hp, tpe, fmin, STATUS_OK, Trials

# COMMAND ----------

# MAGIC %md
# MAGIC #Load the data

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=<SAS-token>")

# COMMAND ----------

file_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/dataprep/cargoIQDF"
data_sample=spark.read.format("parquet").load(file_path)
display(data_sample)

# COMMAND ----------

print(len(data_sample.columns))
data_sample.columns

# COMMAND ----------

data_sample.groupBy('eventCode').count().show()

# COMMAND ----------

cols_to_keep = ['awbNo',
 'departureLocation',
 'arrrivalLocation',
 'total_no_of_pieces',
 'total_weight',
 'total_net_volume',
 'wgt_chargeable',
 'eventCode',
 'planEventDate',
 'eventDate']

# Select the necessary columns
df = data_sample.select(cols_to_keep)
# Print the shape
print((df.count(), len(df.columns)))

# Sort the dataframe
df = df.sort(['awbNo', 'eventDate'])
display(df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Engineering 

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

df = df.withColumn('eventDate', coalesce(col('eventDate'), col('planEventDate')))

window = Window.partitionBy("awbNo").orderBy("eventDate")

time_diff_col = -((col('eventDate').cast('long') - lead(when(col('eventCode') == 'POD', col('eventDate'))).over(window).cast('long')) / 3600).alias('duration_dlv_pod')
# Create target label col: duration between dlv to pod
df = df.withColumn('duration_dlv_pod', when(col('eventCode') == 'DLV', time_diff_col))

# Create col for time difference between each event for the awbNo
df = df.withColumn('eventDate', F.col('eventDate').cast('timestamp') )
df = df.withColumn('eventDate_diff_hours', -(F.unix_timestamp(F.lag("eventDate").over(window)) - F.unix_timestamp(F.col("eventDate"))) / 3600)
df = df.fillna({"eventDate_diff_hours": 0})

time_components = {"weekday": dayofweek, "hour": hour, "month": month, "weekofyear": weekofyear}  #, "year": year, , "dayofmonth": dayofmonth

for key, func in time_components.items():
    df = df.withColumn(f"eventDate_{key}", func(df["eventDate"]))

display(df)

# COMMAND ----------

# Test the logic 
display(df.filter(col("awbNo") == "AAC-13116458"))

# COMMAND ----------

# Filter the data to only have evenCode with DLV
df = df.filter(col("eventCode") == "DLV")

# COMMAND ----------

# Drop unnecessary and correlated features
df = df.drop('awbNo') \
       .drop('eventDate') \
       .drop('planEventDate') \
       .drop('total_weight') \
       .drop('total_net_volume')
#df = df.drop('eventDate_month')

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.na.drop(subset=['duration_dlv_pod'])
target_col = 'duration_dlv_pod'

# COMMAND ----------

#df.groupBy('departureLocation').count().show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create pipeline for data transformation
# MAGIC Imputation of missing values, one-hot encoding, etc

# COMMAND ----------

supported_cols = ["eventDate_diff_hours", "departureLocation", "eventDate_weekday", "total_no_of_pieces", "arrrivalLocation", "eventDate_weekofyear", "eventDate_hour", "wgt_chargeable"]
col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["eventDate_diff_hours", "eventDate_hour", "eventDate_weekday", "eventDate_weekofyear", "wgt_chargeable"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors='coerce'))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["eventDate_diff_hours", "eventDate_weekday", "eventDate_weekofyear", "eventDate_hour", "wgt_chargeable"])]

# COMMAND ----------

one_hot_imputers = []

one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="indicator")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["arrrivalLocation", "departureLocation", "eventDate_weekday", "eventDate_weekofyear", "total_no_of_pieces"])]

# COMMAND ----------

transformers = numerical_transformers + categorical_one_hot_transformers

preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a col for train, test, val split 

# COMMAND ----------

np.random.seed(42)
df = df.toPandas()
train_test_labels = np.random.choice(['train', 'test', 'val'], size = len(df), p = [0.7, 0.2, 0.1])
df['train_test_split'] = train_test_labels

# COMMAND ----------

split_train_df = df.loc[df.train_test_split == "train"]
split_val_df = df.loc[df.train_test_split == "val"]
split_test_df = df.loc[df.train_test_split == "test"]

X_train = split_train_df.drop([target_col, "train_test_split"], axis=1)
y_train = split_train_df[target_col]

X_val = split_val_df.drop([target_col, "train_test_split"], axis=1)
y_val = split_val_df[target_col]

X_test = split_test_df.drop([target_col, "train_test_split"], axis=1)
y_test = split_test_df[target_col]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the objective function
# MAGIC xgboost 

# COMMAND ----------

#help(XGBRegressor)

# COMMAND ----------

pipeline_val = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
])

mlflow.sklearn.autolog(disable=True)
pipeline_val.fit(X_train, y_train)
X_val_processed = pipeline_val.transform(X_val)

def objective(params):
  with mlflow.start_run(experiment_id="130976794464612") as mlflow_run:
    xgb_regressor = XGBRegressor(**params)

    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("regressor", xgb_regressor),
    ])

    mlflow.sklearn.autolog(
        log_input_examples=True,
        silent=True,
    )

    model.fit(X_train, y_train, regressor__early_stopping_rounds=5, regressor__verbose=False, regressor__eval_set=[(X_val_processed,y_val)])

    
    # Log metrics for the training set
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(target_col):y_train}),
        targets=target_col,
        model_type="regressor",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_"}
    )
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(target_col):y_val}),
        targets=target_col,
        model_type="regressor",
        evaluator_config= {"log_model_explainability": False,
                           "metric_prefix": "val_"}
   )
    xgb_val_metrics = val_eval_result.metrics
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test.assign(**{str(target_col):y_test}),
        targets=target_col,
        model_type="regressor",
        evaluator_config= {"log_model_explainability": False,
                           "metric_prefix": "test_"}
   )
    xgb_test_metrics = test_eval_result.metrics

    loss = xgb_val_metrics["val_r2_score"]

    xgb_val_metrics = {k.replace("val_", ""): v for k, v in xgb_val_metrics.items()}
    xgb_test_metrics = {k.replace("test_", ""): v for k, v in xgb_test_metrics.items()}

    return {
      "loss": loss,
      "status": STATUS_OK,
      "val_metrics": xgb_val_metrics,
      "test_metrics": xgb_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure the hyperparameter search space
# MAGIC

# COMMAND ----------

space = {
  "colsample_bytree": 0.5869411822240327,
  "learning_rate": 0.14655365095604356,
  "max_depth": 5,
  "min_child_weight": 1,
  "n_estimators": 223,
  "n_jobs": 100,
  "subsample": 0.7452397368089964,
  "verbosity": 0,
  "random_state": 839458751,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run trials
# MAGIC

# COMMAND ----------

trials = Trials()
fmin(objective,
     space=space,
     algo=tpe.suggest,
     max_evals=1,  
     trials=trials)

best_result = trials.best_trial["result"]
model = best_result["model"]
mlflow_run = best_result["run"]

display(
  pd.DataFrame(
    [best_result["val_metrics"], best_result["test_metrics"]],
    index=["validation", "test"]))

set_config(display="diagram")
model
