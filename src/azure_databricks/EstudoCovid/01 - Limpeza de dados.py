# Databricks notebook source
import  pyspark.sql.functions as F
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

repository_path = "file:/Workspace/Repos/celio.vetrano@gmail.com/databricks/"
data_path = repository_path + "src/azure_databricks/data_covid/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura dos dados que foram previamente extraidos do banco de dados/datalake

# COMMAND ----------

df_covid = spark.read.table('hive_metastore.default.covid_data')
display(df_covid)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movendo dados para camada Bronze

# COMMAND ----------

df_covid.write.parquet(data_path+"bronze", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movendo dados para camada Silver

# COMMAND ----------

silver_df = spark.read.parquet(data_path+"bronze")

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analisando os dados:
# MAGIC   - Remover a coluna das datas de morte, para validar se o paciente morreu (1 para morte e 0 para vivo), criando uma nova coluna DEATH

# COMMAND ----------

from pyspark.sql.types import IntegerType

## DATE_DIED = 9999-99-99 paciente vivo
# Definindo a função died compatível com UDF
def died(x): 
    return 0 if x == '9999-99-99' else 1
# Convertendo a função Python em UDF do Spark
died_udf = udf(died, IntegerType())
# Aplicando a UDF e criando a nova coluna 'DEATH'
silver_df = silver_df.withColumn('DEATH', died_udf('DATE_DIED'))

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df = silver_df.drop('DATE_DIED')

# COMMAND ----------

silver_df.write.parquet(data_path+"silver", mode="overwrite")
