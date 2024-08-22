# Databricks notebook source
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import *

# COMMAND ----------

# Define widgets so values are passed to the notebook
dbutils.widgets.dropdown("data_source", "BLOB", ["BLOB", "API"], "Enter data source (BLOB/API)")

dbutils.widgets.text("secret_scope", "sa-key-vault", "Enter secret scope")
dbutils.widgets.text("storage_acc_name", "saspatiallydata", "Enter Storage account name")
dbutils.widgets.text("container_name", "centers-for-disease-control-and-prevention", "Enter Container name")
dbutils.widgets.text("blob_path", "Nutrition_Physical_Activity_and_Obesity_-_Behavioral_Risk_Factor_Surveillance_System.csv", "Enter blob path")
dbutils.widgets.text("kv_client_id", "sa-client-id", "Enter client id secret name")
dbutils.widgets.text("kv_tenant_id", "sa-tenant-id", "Enter tenant id secret name")
dbutils.widgets.text("kv_secret_value", "sa-secret-value", "Enter secret value secret name")

dbutils.widgets.text("base_url", "https://chronicdata.cdc.gov/resource/hn4x-zwk7.json", "Enter url for API fetching")
dbutils.widgets.text("limit", "10000", "Enter reponse itmes limit")
dbutils.widgets.text("total_records", "93249", "Enter expected total records")

base_url = dbutils.widgets.get("base_url")
limit = int(dbutils.widgets.get("limit"))
total_records = int(dbutils.widgets.get("total_records")) 

# Get widget values
secret_scope = dbutils.widgets.get("secret_scope")
storage_acc_name = dbutils.widgets.get("storage_acc_name")
container_name = dbutils.widgets.get("container_name")
blob_path = dbutils.widgets.get("blob_path")

kv_client_id = dbutils.widgets.get("kv_client_id")
kv_tenant_id = dbutils.widgets.get("kv_tenant_id")
kv_secret_value = dbutils.widgets.get("kv_secret_value")

data_source = dbutils.widgets.get("data_source")


# COMMAND ----------

if data_source == "BLOB":
    # Define Blob file path
    file_path = f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/{blob_path}"

    # Define configuration variables
    client_id = dbutils.secrets.get(scope=secret_scope, key=kv_client_id) 
    tenant_id = dbutils.secrets.get(scope=secret_scope, key=kv_tenant_id) 
    client_secret = dbutils.secrets.get(scope=secret_scope, key=kv_secret_value) 
    
    # Set Spark configurations for OAuth authentication
    spark.conf.set(f"fs.azure.account.auth.type.{storage_acc_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_acc_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_acc_name}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_acc_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_acc_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")



# COMMAND ----------

def fetch_data(base_url, limit, offset):
    query_params = {"$limit": limit, "$offset": offset}
    response = requests.get(base_url, params=query_params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return None
    
def fetch_data_to_df(base_url, limit, total_records):
    all_data = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_data, base_url, limit, offset) for offset in range(0, total_records, limit)]
    
        for future in as_completed(futures):
            data_batch = future.result()
            if data_batch:
                rdd_batch = spark.sparkContext.parallelize(data_batch)
                all_data.append(rdd_batch)

    if all_data:
        full_rdd = spark.sparkContext.union(all_data)
        df = spark.read.json(full_rdd)
        return df
    else:
        raise Exception("No data fetched.")

if data_source == "BLOB":
    df = spark.read.format("csv").option("header", "true").load(file_path)
elif data_source == "API":
    df = fetch_data_to_df(base_url, limit, total_records)
else:
    raise Exception("Invalid data source.")


# COMMAND ----------


if data_source == "BLOB":
    df_transformed = df.withColumn("latitude", trim(regexp_replace(split(col("geolocation"), ",").getItem(0), "[()]", ""))) \
                   .withColumn("longitude", trim(regexp_replace(split(col("geolocation"), ",").getItem(1), "[()]", ""))) \
                   .drop("geolocation")

    df_transformed = df_transformed.withColumn("geolocation", 
                                           struct(lit(None).cast("string").alias("human_address"), 
                                                  col("latitude").cast("double").alias("latitude"), 
                                                  col("longitude").cast("double").alias("longitude")))
else:
    df_transformed = df.withColumn("geolocation", 
                                col("geolocation")
                                .withField("latitude", col("geolocation.latitude").cast("double"))
                                .withField("longitude", col("geolocation.longitude").cast("double"))) \

df_transformed = df_transformed.withColumn("yearstart", col("yearstart").cast("integer")) \
                    .withColumn("yearend", col("yearend").cast("integer")) \
                    .withColumn("data_value", col("data_value").cast("double")) \
                    .withColumn("sample_size", col("sample_size").cast("integer")) \
                    .withColumn("datasource", when(col("datasource") == "Behavioral Risk Factor Surveillance System", "BRFSS")
                                              .otherwise(col("datasource"))) \
                    .withColumn("data_value_type", when(lower(col("question")).contains("percent"), lit("Percent"))
                                                   .otherwise(col("data_value_type")))
                    
if data_source == "BLOB":
    df_transformed = df_transformed.drop("latitude", "longitude")

# Rename and standardize columns to snake_case
df_transformed = df_transformed.withColumnRenamed("yearstart", "year_start") \
                                .withColumnRenamed("yearend", "year_end") \
                                .withColumnRenamed("locationabbr", "location_abbreviation") \
                                .withColumnRenamed("locationdesc", "location_description") \
                                .withColumnRenamed("stratificationcategory1", "stratification_category_1") \
                                .withColumnRenamed("stratification1", "stratification_1")


# Select only the required columns in the final DataFrame
df_transformed = df_transformed.select(
    col("year_start"),
    col("year_end"),
    col("location_abbreviation"),
    col("location_description"),
    col("datasource"),
    col("class"),
    col("data_value_type"),
    col("data_value"),
    col("sample_size"),
    col("geolocation"),
    col("stratification_category_1"),
    col("stratification_1")
)

# Save the intermediate transformed dataset as a global temporary view
df_transformed.createOrReplaceGlobalTempView("cleaned_and_transformed_chronic_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_brfss_doe_john USING DELTA AS
# MAGIC SELECT *
# MAGIC FROM global_temp.cleaned_and_transformed_chronic_data
# MAGIC WHERE year_start >= 2020 
# MAGIC   AND year_end <= 2022
# MAGIC   AND location_abbreviation IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 
# MAGIC                        'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 
# MAGIC                        'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY')
# MAGIC   AND stratification_category_1 NOT RLIKE '(?i)total';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_brfss_doe_john;
# MAGIC
