# Databricks notebook source
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

repository_path = "file:/Workspace/Repos/celio.vetrano@gmail.com/databricks/"
data_path = repository_path + "src/azure_databricks/data_covid/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura dos dados que foram previamente extraidos do banco de dados/datalake

# COMMAND ----------

df_work = spark.read.parquet(data_path + "silver")
df = df_work
##df = df_work.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analise do conteúdo do arquivo

# COMMAND ----------

# Visualizar as primeiras linhas
display(df)

# COMMAND ----------

# Informações gerais sobre o dataset
print(df.info())

# COMMAND ----------

# Estatísticas descritivas
display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agrupando os clientes que tiveram covid, independente do grau e criando a coluna COVID

# COMMAND ----------

 train = df

 train['Covid'] = [0 if i >= 4 else 1 for i in train.CLASIFFICATION_FINAL]

 display(train)


# COMMAND ----------

train = train.withColumn('Covid', F.when(F.col('CLASIFFICATION_FINAL') >= 4, 0).otherwise(1))

display(train)

# COMMAND ----------

## Validando a quantidade de pacientes que tiveram COVID:
display(train.groupBy('Covid').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Limpando e ajustandos os dados

# COMMAND ----------

train

# COMMAND ----------

## Homens não podem estar grávidos, alteraremos os dados incompletos por sem informação:
train = train.withColumn('PREGNANT', F.when(F.col('PREGNANT').isin([97, 98]), -1).otherwise(F.col('PREGNANT')))

display(train)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos transformar os dados inconclusivos sobre o paciente em -1 para ajudar o modelo a chegar a uma análise do risco de morte do paciente

# COMMAND ----------

## alterar dos dados incompletos dos pacientes entubados (INTUBED) para -1
train = train.withColumn('INTUBED', F.when(F.col('INTUBED') > 2, -1).otherwise(F.col('INTUBED')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que adquiriram Pneumonia (PNEUMONIA) para -1
train = train.withColumn('PNEUMONIA', F.when(F.col('PNEUMONIA') > 2, -1).otherwise(F.col('PNEUMONIA')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que são diabéticos (DIABETES) para -1
train = train.withColumn('DIABETES', F.when(F.col('DIABETES') > 2, -1).otherwise(F.col('DIABETES')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que possuiem DPOC (COPD) para -1
train = train.withColumn('COPD', F.when(F.col('COPD') > 2, -1).otherwise(F.col('COPD')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que são asmáticos (ASTHMA) para -1
train = train.withColumn('ASTHMA', F.when(F.col('ASTHMA') > 2, -1).otherwise(F.col('ASTHMA')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que são imunosuprimidos - possuem doença auto-imune (INMSUPR) para -1
train = train.withColumn('INMSUPR', F.when(F.col('INMSUPR') > 2, -1).otherwise(F.col('INMSUPR')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que são hipertensos (HIPERTENSION) para -1
train = train.withColumn('HIPERTENSION', F.when(F.col('HIPERTENSION') > 2, -1).otherwise(F.col('HIPERTENSION')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que possuem outras doenças (OTHER_DISEASE) para -1
train = train.withColumn('OTHER_DISEASE', F.when(F.col('OTHER_DISEASE') > 2, -1).otherwise(F.col('OTHER_DISEASE')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que possuem doenças de coração ou aos vasos sanguíneos (CARDIOVASCULAR) para -1
train = train.withColumn('CARDIOVASCULAR', F.when(F.col('CARDIOVASCULAR') > 2, -1).otherwise(F.col('CARDIOVASCULAR')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que estão fora do peso (OBESITY) para -1
train = train.withColumn('OBESITY', F.when(F.col('OBESITY') > 2, -1).otherwise(F.col('OBESITY')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que possuem doenças renais crônicas (RENAL_CHRONIC) para -1
train = train.withColumn('RENAL_CHRONIC', F.when(F.col('RENAL_CHRONIC') > 2, -1).otherwise(F.col('RENAL_CHRONIC')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que são usuários de tabaco (TOBACCO) para -1
train = train.withColumn('TOBACCO', F.when(F.col('TOBACCO') > 2, -1).otherwise(F.col('TOBACCO')))

display(train)

# COMMAND ----------

## alterar dos dados incompletos dos pacientes que foram internados na UTI (ICU) para -1
train = train.withColumn('ICU', F.when(F.col('ICU') > 2, -1).otherwise(F.col('ICU')))

display(train)

# COMMAND ----------

##copiar o dataset para pandas
df = train.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Histograma
# MAGIC Conceito:
# MAGIC  - Histogramas são usados para representar a distribuição de uma variável contínua, mostrando a frequência de diferentes intervalos de valores.
# MAGIC  - Útil para identificar distribuições (normal, assimétrica, etc.), presença de outliers e compreender a variação dos dados.

# COMMAND ----------

# Histograma para uma variável
df['DEATH'].hist(bins=3, range=(0, 1))
plt.title('Mortes de COVID')
plt.xlabel('Vida / Morte')
plt.ylabel('')
plt.show()

# COMMAND ----------

df["Covid"].hist(bins= 3, range=(0, 1))
plt.title('Casos de COVID')
plt.xlabel('Pessoas com COVID')
plt.ylabel('')
plt.show()

# COMMAND ----------

df["AGE"].hist(bins= 5)
plt.title('Idade dos Pacientes')
plt.xlabel('Idade')
plt.ylabel('Pacientes')
plt.show()

# COMMAND ----------

df['HIPERTENSION'].value_counts()

# COMMAND ----------

df['INTUBED'].value_counts()

# COMMAND ----------

df['PNEUMONIA'].value_counts()

# COMMAND ----------

df['AGE'].value_counts()

# COMMAND ----------

df['PREGNANT'].value_counts()

# COMMAND ----------

df['DIABETES'].value_counts()

# COMMAND ----------

df["PNEUMONIA"].describe()

# COMMAND ----------

## Removendo a coluna Classificação Final
df = df.drop('CLASIFFICATION_FINAL', axis=1)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import matplotlib.pyplot as plt

df.hist(bins=50, figsize=(20,15))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Boxplots
# MAGIC Conceito:
# MAGIC  - Boxplots são úteis para visualizar a distribuição de dados e identificar outliers.
# MAGIC  - Eles mostram a mediana, quartis e valores extremos.

# COMMAND ----------

sns.boxplot(x='DEATH', y='AGE', data=df)
plt.title('Boxplot das mortes por COVID x idade')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scatter Plots (Nuvem de pontos)
# MAGIC Conceito:
# MAGIC  - Scatter plots são utilizados para visualizar a relação entre duas variáveis quantitativas.
# MAGIC  - Úteis para identificar padrões de correlação.

# COMMAND ----------

# Calculando Q1 e Q3
Q1 = df['salario_mensal'].quantile(0.25)
Q3 = df['salario_mensal'].quantile(0.75)
IQR = Q3 - Q1
 
# Definindo limites para outliers
limite_inferior = Q1 - 1.5 * IQR
limite_superior = Q3 + 1.5 * IQR
 
# Removendo os outliers
df_filtrado = df[(df['salario_mensal'] >= limite_inferior) & (df['salario_mensal'] <= limite_superior)]

# COMMAND ----------

sns.scatterplot(data=df_filtrado, x="idade", y="salario_mensal", hue='inadimplente', alpha=0.5)

# COMMAND ----------

sns.scatterplot(data=df, x="idade", y="numero_emprestimos_imobiliarios", hue='inadimplente', alpha=0.2)

# COMMAND ----------

sns.scatterplot(data=df_filtrado, x="salario_mensal", y="numero_emprestimos_imobiliarios", hue='inadimplente', alpha=0.2)

# COMMAND ----------

sns.scatterplot(data=df_filtrado, x="numero_de_dependentes", y="salario_mensal", hue='inadimplente', alpha=0.2)

# COMMAND ----------

plt.scatter(df['idade'], df['numero_emprestimos_imobiliarios'], alpha=0.5)
plt.title('Scatter Plot de Idade vs Quantidade de Emprestimos Imobilários')
plt.xlabel('Idade')
plt.ylabel('Emprestimos Imobilários')
plt.show()

# COMMAND ----------

# Calculando Q1 e Q3
Q1 = df['salario_mensal'].quantile(0.25)
Q3 = df['salario_mensal'].quantile(0.75)
IQR = Q3 - Q1
 
# Definindo limites para outliers
limite_inferior = Q1 - 1.5 * IQR
limite_superior = Q3 + 1.5 * IQR
 
# Removendo os outliers
df_filtrado = df[(df['salario_mensal'] >= limite_inferior) & (df['salario_mensal'] <= limite_superior)]

# COMMAND ----------

plt.scatter(df_filtrado['idade'], df_filtrado['salario_mensal'])
plt.title('Scatter Plot de Idade vs Renda')
plt.xlabel('Idade')
plt.ylabel('Renda')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Histograma (KDE) para inadimplência

# COMMAND ----------

plt.figure(figsize = (10, 8))

# KDE plot of loans that were repaid on time
sns.kdeplot(df.loc[df['inadimplente'] == 0, 'idade'], label = 'Bom pagador')

# KDE plot of loans which were not repaid on time
sns.kdeplot(df.loc[df['inadimplente'] == 1, 'idade'], label = 'Mau pagador')

# Labeling of plot
plt.xlabel('Idade (anos)'); 
plt.ylabel('Density'); 
plt.title('Distribuição das idades');
plt.legend();

# COMMAND ----------

plt.figure(figsize = (10, 8))

# KDE plot of loans that were repaid on time
sns.kdeplot(df.loc[df['inadimplente'] == 0, 'numero_emprestimos_imobiliarios'], label = 'Bom pagador')

# KDE plot of loans which were not repaid on time
sns.kdeplot(df.loc[df['inadimplente'] == 1, 'numero_emprestimos_imobiliarios'], label = 'Mau pagador')

# Labeling of plot
plt.xlabel('numero_emprestimos_imobiliarios (anos)'); 
plt.ylabel('Density'); 
plt.title('Distribuição das numero_emprestimos_imobiliarios');
plt.legend();

# COMMAND ----------

plt.figure(figsize = (10, 8))

# KDE plot of loans that were repaid on time
sns.kdeplot(df.loc[df['inadimplente'] == 0, 'numero_linhas_crdto_aberto'], label = 'Bom pagador')

# KDE plot of loans which were not repaid on time
sns.kdeplot(df.loc[df['inadimplente'] == 1, 'numero_linhas_crdto_aberto'], label = 'Mau pagador')

# Labeling of plot
plt.xlabel('numero_linhas_crdto_aberto (anos)'); 
plt.ylabel('Density'); 
plt.title('Distribuição das idades');
plt.legend();

# COMMAND ----------

# MAGIC %md
# MAGIC #### Correlação
# MAGIC Conceito:
# MAGIC  - A correlação mede a relação estatística entre duas variáveis.
# MAGIC  - O coeficiente de correlação varia entre -1 e 1. Valores próximos a 1 indicam forte correlação positiva, enquanto valores próximos a -1 indicam forte correlação negativa.

# COMMAND ----------

df[['USMER', 'MEDICAL_UNIT', 'SEX', 'PATIENT_TYPE', 'INTUBED', 'PNEUMONIA', 'AGE', 'PREGNANT', 'DIABETES', 'COPD', 'ASTHMA', 'INMSUPR', 'HIPERTENSION', 'OTHER_DISEASE', 'CARDIOVASCULAR', 'OBESITY', 'RENAL_CHRONIC', 'TOBACCO', 'ICU', 'Covid', 'DEATH']].corr()

# COMMAND ----------

df['idade_cat'] = pd.cut(df['idade'], bins=list(range(18, 81, 5)), right=False)

# COMMAND ----------

df['DEATH'].value_counts(normalize=True).sort_index().plot.bar()

# COMMAND ----------

df.groupby('idade_cat')['inadimplente'].mean().plot()

# COMMAND ----------

df_filtrado[['inadimplente','idade', 'salario_mensal', 'numero_linhas_crdto_aberto', 'numero_emprestimos_imobiliarios']].corr('spearman')

# COMMAND ----------

df.corr()

# COMMAND ----------

ax, fig = plt.subplots(figsize=(16, 16))
sns.heatmap(df_filtrado.drop(['inadimplente'], axis=1).corr(), annot=True, fmt=".2f")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Construção de novas variáveis pertinentes para nosso problema

# COMMAND ----------

# Criação de novas variáveis (feature engineering)

# 2. Interação entre idade e número de linhas de crédito
df['idade_x_linhas_credito'] = df['idade'] * df['numero_linhas_crdto_aberto']

# 3. Razão entre salário mensal e dívida
df['razao_salario_debito'] = df['salario_mensal'] / df['razao_debito']
df['razao_salario_debito'] = df['razao_salario_debito'].replace([np.inf, -np.inf], np.nan)  # Tratamento para divisão por zero

# 4. Total de atrasos
df['total_atrasos'] = df['vezes_passou_de_30_59_dias'] + df['numero_vezes_passou_90_dias'] + df['numero_de_vezes_que_passou_60_89_dias']

# 5. Indicador de dados faltantes
df['salario_mensal_faltante'] = df['salario_mensal'].isnull().astype(int)

# Exibir as primeiras linhas do DataFrame com as novas variáveis
df.head()

# COMMAND ----------

df['inadimplente']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Após a construção de novas informações salvamos o novo dataset na nossa camada `gold`

# COMMAND ----------

df_silver = spark.createDataFrame(df)

# COMMAND ----------

df_silver.write.parquet(data_path+"gold", mode="overwrite")
