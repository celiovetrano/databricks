# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tripdata/yellow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos dados necessários

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz", inferSchema=True, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Para facilitar a execução das atividades, quem tiver maior familiaridade com SQL poderá utilizar o comando abaixo que nos permite executar queries SQL diretamente

# COMMAND ----------

df.createOrReplaceTempView("taxi_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC    SELECT *
# MAGIC     FROM taxi_trips
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("taxi_trips")

query = """
    SELECT *
    FROM taxi_trips
"""

spark.sql(query).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perguntas de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 1: Quais são os horários de maior demanda por táxis em Nova York?

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(tpep_pickup_datetime, 'H') AS pickup_time,
# MAGIC   COUNT(*) AS num_trips
# MAGIC   FROM taxi_trips
# MAGIC   GROUP BY
# MAGIC   pickup_time
# MAGIC   ORDER BY
# MAGIC   num_trips DESC, pickup_time

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 2: Qual é a média de distância percorrida por viagem em cada dia da semana?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   dayofweek(tpep_pickup_datetime) as day_of_week,
# MAGIC   avg(trip_distance)
# MAGIC from taxi_trips
# MAGIC group by
# MAGIC   day_of_week
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 3: Quantas corridas foram pagas em dinheiro por cada dia da semana?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(VendorID), date_format (tpep_dropoff_datetime, 'E') a
# MAGIC     FROM taxi_trips
# MAGIC     WHERE payment_type == 2
# MAGIC     group by a
# MAGIC     order by count(VendorId)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 4: Qual é a proporção de viagens curtas (< 2 milhas) para viagens longas (>= 2 milhas) durante os dias úteis?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     DAYOFWEEK(tpep_pickup_datetime) AS diaSemana,
# MAGIC     SUM(CASE WHEN trip_distance < 2 THEN 1 ELSE 0 END) AS viagemCurta,
# MAGIC     SUM(CASE WHEN trip_distance >= 2 THEN 1 ELSE 0 END) AS viagemLonga,
# MAGIC     COUNT(*) AS totalViagem
# MAGIC FROM
# MAGIC     taxi_trips
# MAGIC WHERE
# MAGIC     DAYOFWEEK(tpep_pickup_datetime) BETWEEN 2 AND 6
# MAGIC GROUP BY
# MAGIC     diaSemana
# MAGIC ORDER BY
# MAGIC     diaSemana;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 5: Em quais horários os passageiros dão as melhores gorjetas?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(tpep_pickup_datetime, 'H') AS hora,
# MAGIC   avg(tip_amount) AS melhor_gorjeta
# MAGIC FROM
# MAGIC   taxi_trips
# MAGIC GROUP BY
# MAGIC   hora
# MAGIC ORDER BY
# MAGIC   melhor_gorjeta DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(tpep_dropoff_datetime, 'H') AS hora,
# MAGIC   avg(tip_amount) AS melhor_gorjeta
# MAGIC FROM
# MAGIC   taxi_trips
# MAGIC GROUP BY
# MAGIC   hora
# MAGIC ORDER BY
# MAGIC   melhor_gorjeta DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta: Quais são os cinco principais locais de partida (PULocationID) que resultam na maior receita total?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select round(sum(total_amount), 2) as maior_receita
# MAGIC       ,PULocationID
# MAGIC from taxi_trips
# MAGIC group by PULocationID
# MAGIC order by maior_receita desc
# MAGIC limit 5
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 6: Qual é o valor médio das corridas por tipo de pagamento?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 7: Como a distância média das viagens varia ao longo do dia?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT CAST(EXTRACT(HOUR FROM tpep_pickup_datetime) AS INT) AS HoraDoDia, AVG(trip_distance) AS DistanciaMedia
# MAGIC FROM taxi_trips
# MAGIC GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
# MAGIC ORDER BY HoraDoDia ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 8: Qual é a duração média das corridas que começam e terminam na mesma localização?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(PULocationID)
# MAGIC from taxi_trips
# MAGIC WHERE PULocationID = DOLocationID

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC from taxi_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     round(avg(datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime)), 2) AS duracao_media_min,
# MAGIC     PULocationID
# MAGIC FROM 
# MAGIC     taxi_trips
# MAGIC WHERE PULocationID = DOLocationID
# MAGIC GROUP BY PULocationID
# MAGIC order by duracao_media_min desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     round(avg(datediff(hour, tpep_pickup_datetime, tpep_dropoff_datetime)), 2) AS duracao_media_hh,
# MAGIC     PULocationID
# MAGIC FROM 
# MAGIC     taxi_trips
# MAGIC WHERE PULocationID = DOLocationID
# MAGIC GROUP BY PULocationID
# MAGIC order by duracao_media_hh desc

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 9. Qual a proporção de viagens com apenas um passageiro em relação ao total de viagens?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 10. Quais são os top 3 horários de pico para início das corridas?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 11. Qual é a variação percentual na quantidade de viagens entre dias úteis e fins de semana?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 12. Quais são os 5 destinos mais comuns nas corridas que excedem $50?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 13. Como a distância média e o valor médio da gorjeta estão relacionados?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pergunta 14. Qual é o tempo médio de viagem entre as localizações mais populares de embarque e desembarque?
