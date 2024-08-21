# Databricks notebook source
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col, when, lit, lower

# COMMAND ----------

def fetch_data(base_url, limit, offset):
    query_params = {"$limit": limit, "$offset": offset}
    response = requests.get(base_url, params=query_params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return None
    

base_url = "https://chronicdata.cdc.gov/resource/hn4x-zwk7.json"
limit = 10000
total_records = 93249 
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
    display(df)
    print(f"Total records: {df.count()}")
else:
    raise Exception("No data fetched.")


# COMMAND ----------


df_transformed = df.withColumn("yearstart", col("yearstart").cast("integer")) \
                    .withColumn("yearend", col("yearend").cast("integer")) \
                    .withColumn("data_value", col("data_value").cast("double")) \
                    .withColumn("sample_size", col("sample_size").cast("integer")) \
                    .withColumn("geolocation", 
                                col("geolocation")
                                .withField("latitude", col("geolocation.latitude").cast("double"))
                                .withField("longitude", col("geolocation.longitude").cast("double"))) \
                    .withColumn("datasource", when(col("datasource") == "Behavioral Risk Factor Surveillance System", "BRFSS")
                                              .otherwise(col("datasource"))) \
                    .withColumn("data_value_type", when(lower(col("question")).contains("percent"), lit("Percent"))
                                                   .otherwise(col("data_value_type")))

# Replace inappropriate values such as '~' with null
# df = df.withColumn("data_value", when(col("data_value") == "~", None).otherwise(col("data_value"))) \
#        .withColumn("data_value_footnote_symbol", when(col("sample_size") == "~", None).otherwise(col("sample_size")))

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
display(df_transformed)

# COMMAND ----------


# Predefined list of US states
us_states = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# Filter the data for the required years, states, and remove the 'Total' records
df_filtered = df_transformed.filter((col("year_start") >= 2020) & (col("year_end") <= 2022)) \
                        .filter(col("location_abbreviation").isin(us_states)) \
                        .filter(~col("stratification_category_1").rlike("(?i)total"))

# Save the filtered dataset in a Delta Lake table 
table_name = "gold_brfss_doe_john"
df_filtered.write.format("delta").mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_brfss_doe_john;
# MAGIC
