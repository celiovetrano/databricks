# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`covid_data`;

# COMMAND ----------

df_covid = spark.read.table('hive_metastore.default.covid_data')

display(df_covid)
