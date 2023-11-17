# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`covid_data`;

# COMMAND ----------

df_covid = spark.read.table('hive_metastore.default.covid_data')

display(df_covid)

# COMMAND ----------

import  pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------


