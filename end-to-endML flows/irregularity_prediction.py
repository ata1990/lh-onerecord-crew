# Databricks notebook source
# MAGIC %md
# MAGIC #Irregularity prediction
# MAGIC ### This notebook contains data preprocessing, feature engineering and model training for prediction of irregularity 

# COMMAND ----------

# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, regexp_replace, abs
from pyspark.ml import Pipeline
from pyspark.sql.functions import dayofweek, hour, month, year, weekofyear, dayofmonth
from pyspark.sql.types import TimestampType
import mlflow
import databricks.automl_runtime
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from databricks.automl_runtime.sklearn import OneHotEncoder
import pandas as pd
import numpy as np
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
from sklearn import set_config
from hyperopt import hp, tpe, fmin, STATUS_OK, Trials
from sklearn.ensemble import RandomForestClassifier
import sklearn
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.preprocessing import LabelEncoder
from databricks.automl_runtime.sklearn import TransformedTargetClassifier
from pyspark.sql.functions import col, regexp_replace, when, monotonically_increasing_id, dayofweek, hour, month, weekofyear, lead, unix_timestamp
import mlflow.sklearn


# COMMAND ----------

# MAGIC %md
# MAGIC #Load the data

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=<SAS-token>")

# COMMAND ----------

file_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/dataprep/firstDraft500AWB"
data_sample=spark.read.format("parquet").load(file_path)
print(len(data_sample.columns))
print(data_sample.columns)
display(data_sample)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data preparation

# COMMAND ----------

cols_to_keep = ['awbNo',
 'wgt_chargeable',
 'departureLocation',
 'arrrivalLocation',
 'total_no_of_pieces',
 'product_code',
 'nature_of_goods',
 'commodity',
 'delay',
 'eventDate',
 'eventCode'
 ] 

# Select the necessary columns
df = data_sample.select(cols_to_keep)
# Print the shape
print((df.count(), len(df.columns)))
 
# Create the binary col for eventCode
df.groupBy('eventCode').count().show()
df = df.withColumn("statusBinary", F.when(F.col('eventCode') == 'DIS', 1).otherwise(0))
 
# Sort the dataframe
df = df.sort(['awbNo', 'eventDate'])
display(df) 

# COMMAND ----------

# Test the logic
filtered_df = df.filter(col('awbNo') == 'FRA-41529563')
filtered_df = filtered_df.toPandas()
filtered_df = filtered_df.drop(['statusBinary'], axis = 1)
display(filtered_df)

# COMMAND ----------

# Correct the data: remove minus sign and convert colons to dots
df = df.withColumn("delay_h", regexp_replace(col("delay"), ":", "."))
df = df.withColumn("delay_h", regexp_replace(col("delay"), "-", ""))
df = df.drop("delay")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering 

# COMMAND ----------

# Convert date column to DateType to extract new features 
df = df.withColumn("eventDate", df["eventDate"].cast(TimestampType()))

time_components = {"weekday": dayofweek, "hour": hour, "month": month, "weekofyear": weekofyear}  #, "year": year, , "dayofmonth": dayofmonth

for key, func in time_components.items():
    df = df.withColumn(f"eventDate_{key}", func(df["eventDate"]))

df = df.withColumn("index", F.monotonically_increasing_id())
 
# Create new column 'statusTarget' as shifted 'statusBinary' grouped by 'awbNo'
# statusTarget is the label of classification problem. If the irregulariy happens in the next step with the current status and information of data
window = Window.partitionBy("awbNo").orderBy("index")
df = df.withColumn("statusTarget", F.lead(df["statusBinary"]).over(window))
df = df.fillna({"statusTarget": 0})

# create a new column: duration between the current status and the nex status in hours
df = df.withColumn('eventDate', F.col('eventDate').cast('timestamp') )
df = df.withColumn('eventDate_diff_hours', (F.unix_timestamp(F.lead("eventDate").over(window)) - F.unix_timestamp(F.col("eventDate"))) / 3600)
df = df.fillna({"eventDate_diff_hours": 0})

#
cols_to_drop = ["index", "eventDate"]
df = df.drop(*cols_to_drop)
print(df.columns)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# List of numerical columns 
#num_cols = ['wgt_chargeable', 'vol_gross', 'total_no_of_pieces', 'total_weight', 'total_net_volume', 'delay', 'eventDate_diff_hours']
# Some cols have been deleted due to the high correlation observed in EDA process
num_cols = ['wgt_chargeable', 'total_no_of_pieces', 'delay_h', 'eventDate_diff_hours']

# List of categorical columns
cat_cols = ['departureLocation', 'arrrivalLocation', 'product_code', 'nature_of_goods', 'commodity', 'eventCode', 'statusBinary', 'statusTarget', 'eventDate_weekday', 'eventDate_hour', 'eventDate_month', 'eventDate_weekofyear'] 

# Converting the numerical columns
for col_name in num_cols:
    df = df.withColumn(col_name, col(col_name).cast("double"))

# Converting the categorical columns
for col_name in cat_cols:
    df = df.withColumn(col_name, col(col_name).cast("string"))


# COMMAND ----------

len(df.columns)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create pipeline for data transformation
# MAGIC Imputation of missing values, one-hot encoding, etc

# COMMAND ----------

target_col = "statusTarget"
from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
supported_cols = ["commodity", "product_code", "eventDate_diff_hours", "departureLocation", "statusBinary", "eventDate_weekday", "total_no_of_pieces", "arrrivalLocation", "eventDate_weekofyear", "eventDate_hour", "nature_of_goods", "wgt_chargeable", "eventCode"]
col_selector = ColumnSelector(supported_cols)
col_selector

# COMMAND ----------

# pipeline for boolean features
bool_imputers = []

bool_pipeline = Pipeline(steps=[
    ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
    ("imputers", ColumnTransformer(bool_imputers, remainder="passthrough")),
    ("onehot", SklearnOneHotEncoder(handle_unknown="ignore", drop="first")),
])

bool_transformers = [("boolean", bool_pipeline, ["statusBinary"])]

# COMMAND ----------

# pipeline for numerical features
num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["eventDate_diff_hours", "total_no_of_pieces", "wgt_chargeable"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors='coerce'))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["total_no_of_pieces", "eventDate_diff_hours", "wgt_chargeable"])]

# COMMAND ----------

# pipeline for categorical features
one_hot_imputers = []

one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="indicator")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["arrrivalLocation", "commodity", "departureLocation", "eventCode", "eventDate_hour", "eventDate_weekday", "eventDate_weekofyear", "nature_of_goods", "product_code"])]

# COMMAND ----------

transformers = bool_transformers + numerical_transformers + categorical_one_hot_transformers
preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a col for train, test, val split 

# COMMAND ----------

df= df.toPandas()
np.random.seed(42)

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

from sklearn.tree import DecisionTreeClassifier
#help(DecisionTreeClassifier)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision Tree
# MAGIC ### Define the objective function, mlflow_run

# COMMAND ----------

def objective(params):
  with mlflow.start_run(experiment_id="3458010440362148") as mlflow_run:
    skdtc_classifier = DecisionTreeClassifier(**params)

    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("classifier", skdtc_classifier),
    ])

    mlflow.sklearn.autolog(
        log_input_examples=True,
        silent=True)

    model.fit(X_train, y_train)
    
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(target_col):y_train}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": "1" }
    )
    skdtc_training_metrics = training_eval_result.metrics
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(target_col):y_val}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": "1" }
    )
    skdtc_val_metrics = val_eval_result.metrics
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test.assign(**{str(target_col):y_test}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_" , "pos_label": "1" }
    )
    skdtc_test_metrics = test_eval_result.metrics

    loss = skdtc_val_metrics["val_recall_score"]

    skdtc_val_metrics = {k.replace("val_", ""): v for k, v in skdtc_val_metrics.items()}
    skdtc_test_metrics = {k.replace("test_", ""): v for k, v in skdtc_test_metrics.items()}

    return {
      "loss": loss,
      "status": STATUS_OK,
      "val_metrics": skdtc_val_metrics,
      "test_metrics": skdtc_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

# Configure hyperparameter space
space = {
  "criterion": "entropy",
  "max_depth": 8,
  "max_features": 0.7962628514833345,
  "min_samples_leaf": 0.14658322735586096,
  "min_samples_split": 0.08848775933507519,
  "random_state": 143861876,
}

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature importance 

# COMMAND ----------

# update it with True ( time-intensive, put it False for now)
shap_enabled = False
if shap_enabled:
    mlflow.autolog(disable=True)
    mlflow.sklearn.autolog(disable=True)
    from shap import KernelExplainer, summary_plot
    mode = X_train.mode().iloc[0]

    train_sample = X_train.sample(n=min(100, X_train.shape[0]), random_state=143861876).fillna(mode)

    example = X_val.sample(n=min(100, X_val.shape[0]), random_state=143861876).fillna(mode)

    predict = lambda x: model.predict_proba(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="logit")
    shap_values = explainer.shap_values(example, l1_reg=False, nsamples=500)
    summary_plot(shap_values, example, class_names=model.classes_)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random Forest
# MAGIC ### Define the objective function, mlflow_run

# COMMAND ----------

#help(RandomForestClassifier)

# COMMAND ----------

def objective(params):
  with mlflow.start_run(experiment_id="3458010440362148") as mlflow_run:
    skrf_classifier = RandomForestClassifier(n_jobs=1, **params)

    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("classifier", skrf_classifier),
    ])

    mlflow.sklearn.autolog(
        log_input_examples=True,
        silent=True)

    model.fit(X_train, y_train)

    
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(target_col):y_train}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": "1" }
    )
    skrf_training_metrics = training_eval_result.metrics
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(target_col):y_val}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": "1" }
    )
    skrf_val_metrics = val_eval_result.metrics
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test.assign(**{str(target_col):y_test}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_" , "pos_label": "1" }
    )
    skrf_test_metrics = test_eval_result.metrics

    loss = skrf_val_metrics["val_recall_score"]

    skrf_val_metrics = {k.replace("val_", ""): v for k, v in skrf_val_metrics.items()}
    skrf_test_metrics = {k.replace("test_", ""): v for k, v in skrf_test_metrics.items()}

    return {
      "loss": loss,
      "status": STATUS_OK,
      "val_metrics": skrf_val_metrics,
      "test_metrics": skrf_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

space = {
  "bootstrap": True,
  "criterion": "gini",
  "max_depth": 9,
  "max_features": 0.5077602759357688,
  "min_samples_leaf": 0.12336498657198877,
  "min_samples_split": 0.2917342067395503,
  "n_estimators": 252,
  "random_state": 143861876,
}

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logistic Regression
# MAGIC ### Define the objective function, mlflow_run

# COMMAND ----------

#help(LogisticRegression)

# COMMAND ----------

def objective(params):
  with mlflow.start_run(experiment_id="3458010440362148") as mlflow_run:
    sklr_classifier = LogisticRegression(**params)

    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("classifier", sklr_classifier),
    ])

    mlflow.sklearn.autolog(
        log_input_examples=True,
        silent=True)

    model.fit(X_train, y_train)

    
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(target_col):y_train}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": "1" }
    )
    sklr_training_metrics = training_eval_result.metrics
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(target_col):y_val}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": "1" }
    )
    sklr_val_metrics = val_eval_result.metrics
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test.assign(**{str(target_col):y_test}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_" , "pos_label": "1" }
    )
    sklr_test_metrics = test_eval_result.metrics

    loss = sklr_val_metrics["val_recall_score"]

    sklr_val_metrics = {k.replace("val_", ""): v for k, v in sklr_val_metrics.items()}
    sklr_test_metrics = {k.replace("test_", ""): v for k, v in sklr_test_metrics.items()}

    return {
      "loss": loss,
      "status": STATUS_OK,
      "val_metrics": sklr_val_metrics,
      "test_metrics": sklr_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

space = {
  "C": 0.02266778809311503,
  "l1_ratio": 0.9941203224190954,
  "penalty": "elasticnet",
  "solver": "saga",
  "random_state": 143861876,
}

# COMMAND ----------

trials = Trials()
fmin(objective,
     space=space,
     algo=tpe.suggest,
     max_evals=1,  # Increase this when widening the hyperparameter search space.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## xgboost
# MAGIC ### Define the objective function, mlflow_run

# COMMAND ----------

#help(XGBClassifier)

# COMMAND ----------

mlflow.sklearn.autolog(disable=True)
pipeline_val = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
])
pipeline_val.fit(X_train, y_train)
X_val_processed = pipeline_val.transform(X_val)
label_encoder_val = LabelEncoder()
label_encoder_val.fit(y_train)
y_val_processed = label_encoder_val.transform(y_val)

def objective(params):
  with mlflow.start_run(experiment_id="3458010440289647") as mlflow_run:
    xgbc_classifier = TransformedTargetClassifier(
        classifier=XGBClassifier(**params),
        transformer=LabelEncoder()  
    )

    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("classifier", xgbc_classifier),
    ])

    mlflow.sklearn.autolog(
        log_input_examples=True,
        silent=True)

    model.fit(X_train, y_train, classifier__early_stopping_rounds=5, classifier__verbose=False, classifier__eval_set=[(X_val_processed,y_val_processed)])

    
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(target_col):y_train}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": "1" }
    )
    xgbc_training_metrics = training_eval_result.metrics
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(target_col):y_val}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": "1" }
    )
    xgbc_val_metrics = val_eval_result.metrics
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test.assign(**{str(target_col):y_test}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_" , "pos_label": "1" }
    )
    xgbc_test_metrics = test_eval_result.metrics

    loss = xgbc_val_metrics["val_recall_score"]

    xgbc_val_metrics = {k.replace("val_", ""): v for k, v in xgbc_val_metrics.items()}
    xgbc_test_metrics = {k.replace("test_", ""): v for k, v in xgbc_test_metrics.items()}

    return {
      "loss": loss,
      "status": STATUS_OK,
      "val_metrics": xgbc_val_metrics,
      "test_metrics": xgbc_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

space = {
  "colsample_bytree": 0.5835502613551707,
  "learning_rate": 2.4093093817530082,
  "max_depth": 3,
  "min_child_weight": 4,
  "n_estimators": 9,
  "n_jobs": 100,
  "subsample": 0.6088823721160945,
  "verbosity": 0,
  "random_state": 143861876,
}

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

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define a function for preprocessing the new data for irreg_probabilty scoring function

# COMMAND ----------

def prepare_data(df):
    df = df.withColumn("statusBinary", when(col('eventCode') == 'DIS', 1).otherwise(0))

    df = df.withColumn("delay_h", regexp_replace(col("delay"), ":", "."))
    df = df.withColumn("delay_h", regexp_replace(col("delay"), "-", ""))

    df = df.drop("delay")

    df = df.withColumn("eventDate", df["eventDate"].cast(TimestampType()))

    time_components = {"weekday": dayofweek, "hour": hour, "month": month, "weekofyear": weekofyear}
    for key, func in time_components.items():
        df = df.withColumn(f"eventDate_{key}", func(df["eventDate"]))

    df = df.withColumn("index", monotonically_increasing_id())

    window = Window.partitionBy("awbNo").orderBy("index")
    df = df.withColumn("statusTarget", lead(df["statusBinary"]).over(window))

    df = df.fillna({"statusTarget": 0})

    df = df.withColumn('eventDate', col('eventDate').cast('timestamp'))
    df = df.withColumn('eventDate_diff_hours', (unix_timestamp(lead("eventDate").over(window)) - unix_timestamp(col("eventDate"))) / 3600)

    df = df.fillna({"eventDate_diff_hours": 0})

    cols_to_drop = ["awbNo", "index", "eventDate"]
    df = df.drop(*cols_to_drop)

    return df

# COMMAND ----------

testdata = spark.read.format("delta").load("abfss://<container>@<storage-account>.dfs.core.windows.net/sample_load_awb")
display(testdata)

# COMMAND ----------

testdata = testdata.withColumnRenamed('HAWB', 'awbNo')
testing = testdata.toPandas()
col_names = testing.columns.tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the irreg_prob function

# COMMAND ----------

def irreg_prob(input_data, column_names, logged_model):
    # Load the best model using mlflow
    best_model = mlflow.sklearn.load_model(logged_model)
    input_temp = spark.createDataFrame(input_data)
    input_temp2 = prepare_data(input_temp)
    input_temp3 = input_temp2.toPandas()
    # Make probability predictions using the loaded model
    predictions = best_model.predict_proba(input_temp3)
    
    # Create DataFrame with column names
    res_df = pd.DataFrame(input_data, columns=column_names[:-1])  
    res_df['irregProbability'] = predictions[:, 1]  

    return res_df

#logged_model = 'runs:/f45e7fd6de1f44bd95fdf8af320d9666/model'
logged_model = 'runs:/212fd38042fe4d078139fd5573aa2cc7/model'  
#logged_model = 'runs:/13d1e2867d3b4389a5480522b80fc560/model'


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create an output of irreg_prob function 
# MAGIC which is basically the dataframe with input data plus the probabilty of irregularity in the next step

# COMMAND ----------

results = irreg_prob(testing, col_names, logged_model)
results

# COMMAND ----------

#from pyspark.sql.window import Window

# COMMAND ----------

spark_results = spark.createDataFrame(results)
# Save Spark DataFrame as Delta table
spark_results.write.format("delta").mode("overwrite").saveAsTable("outputs")
