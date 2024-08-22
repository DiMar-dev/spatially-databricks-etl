# Databricks notebook source
# MAGIC %md
# MAGIC # [Spatially DB test task] ETL Development on Databricks and Delta Lake Task
# MAGIC
# MAGIC ### Introduction
# MAGIC
# MAGIC This Databricks notebook implements an ETL (Extract, Transform, Load) pipeline designed to ingest data from two different sources: an API and Azure Blob Storage. The goal is to process the data, perform necessary transformations, and store the final cleaned dataset into a Delta table.
# MAGIC
# MAGIC ####Resource Setup:
# MAGIC    - **App Registration**: For secure and managed access to Azure resources, an App Registration was created, which acts as a Service Principal. This Service Principal is used to authenticate and authorize access to the Storage account.
# MAGIC    - **Key Vault for Secret Management**: To securely store and manage sensitive information like client secrets and API keys, an Azure Key Vault was set up. The secrets stored in the Key Vault are retrieved dynamically during the execution of the pipeline.
# MAGIC    - **Azure Storage Account**: An Azure Storage Account was created to store the CSV extract. This storage account is accessed directly from the Databricks environment using the credentials securely managed in the Key Vault.
# MAGIC
# MAGIC This approach ensures that the pipeline is both flexible and efficient, capable of handling  datasets from different sources while maintaining data integrity and security through the use of Azure services like Key Vault and App Registration.
# MAGIC
# MAGIC

# COMMAND ----------

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Widgets Initialization** 
# MAGIC
# MAGIC This section defines the widgets that capture user inputs for configuring the data source and other parameters.
# MAGIC
# MAGIC **Dynamic Data Source Selection**:
# MAGIC    The pipeline is designed to be flexible, allowing the user to choose between two data sources: Azure Blob Storage and an API. This is controlled through the use of Databricks widgets, which provide a user-friendly interface to input parameters such as the data source, storage account details, API endpoints, and other necessary configurations.
# MAGIC

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

# MAGIC %md
# MAGIC **Data Ingestion**
# MAGIC
# MAGIC Depending on the selected data source, the notebook either connects to Azure Blob Storage or fetches data from the API.
# MAGIC
# MAGIC    - **Blob Storage**: If the user selects Blob Storage as the data source, the notebook connects to the specified Azure Storage account using OAuth authentication via the Service Principal. The data is then read directly from a CSV file stored in the Blob container.
# MAGIC    - **API**: If the API is selected, the notebook fetches the data in batches using Python’s `requests` library. To handle large datasets efficiently, the data retrieval process is parallelized using `ThreadPoolExecutor`.
# MAGIC

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

# MAGIC %md
# MAGIC **Data Transformation**: This section includes the logic for transforming the raw data into a standardized format suitable for storage.
# MAGIC
# MAGIC **Data Transformation**:
# MAGIC    The data is transformed based on its source:
# MAGIC    - For data ingested from Blob Storage, the pipeline extracts geolocation information from a combined string field, splitting it into latitude and longitude, and then reconstructs it into a JSON format as one commign form the API.
# MAGIC    - For API data, geolocation fields are directly cast to appropriate data types.
# MAGIC    - Additional transformations include standardizing column names to snake_case, casting data types for consistency, and refining specific fields based on their content (e.g., categorizing data value types).
# MAGIC    - There were fields having `~` which should be replaced with `null` but as those columns weren't used the step was also skipped.
# MAGIC

# COMMAND ----------


if data_source == "BLOB":
    # If source is Blob then we need to transform the geolocation column to struct including human_address, latitude and longitude.
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

# MAGIC %md
# MAGIC **Data Loading**: The transformed data is saved into a Delta table with appropriate filters applied.
# MAGIC
# MAGIC **Data Loading**:
# MAGIC    The final step involves saving the cleaned and transformed data into a Delta table. A SQL query is used to filter the data for specific years and locations before storing it, ensuring that only relevant information is retained.
# MAGIC
# MAGIC *same could be achieved using this Python code also:
# MAGIC ```python
# MAGIC # Predefined list of US states
# MAGIC us_states = [
# MAGIC     "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
# MAGIC     "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
# MAGIC     "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
# MAGIC ]
# MAGIC
# MAGIC # Filter the data for the required years, states, and remove the 'Total' records
# MAGIC df_filtered = df_transformed.filter((col("year_start") >= 2020) & (col("year_end") <= 2022)) \
# MAGIC                         .filter(col("location_abbreviation").isin(us_states)) \
# MAGIC                         .filter(~col("stratification_category_1").rlike("(?i)total"))
# MAGIC
# MAGIC # Save the filtered dataset in a Delta Lake table 
# MAGIC table_name = "gold_brfss_dimitrijeski_marin"
# MAGIC df_filtered.write.format("delta").mode("overwrite").saveAsTable(table_name)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_brfss_dimitrijeski_marin USING DELTA AS
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
# MAGIC SELECT * FROM gold_brfss_dimitrijeski_marin;
# MAGIC
