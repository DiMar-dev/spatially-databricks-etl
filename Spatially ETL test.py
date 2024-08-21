# Databricks notebook source
from pyspark.sql import SparkSession

df = spark.range(1, 11).toDF("Numbers")

display(df)
