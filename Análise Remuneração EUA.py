# Databricks notebook source
# MAGIC %md
# MAGIC Comparando os extremos, os profissionais acima de 50 anos ganham cerca de 80% mais do que os da faixa 20–25 anos.
# MAGIC
# MAGIC O crescimento médio entre cada faixa etária é de aproximadamente 10% a 15%, mostrando uma progressão salarial consistente e proporcional ao avanço da idade.
# MAGIC
# MAGIC Essa relação confirma uma correlação positiva entre idade e salário, reforçando que o tempo de experiência é um dos principais determinantes da remuneração.

# COMMAND ----------

from pyspark.sql.functions import when, col, avg
import matplotlib.pyplot as plt

# 📥 Leitura do arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Drafts/employee_salary_regression.csv", header=True, inferSchema=True)

# 🔄 Criar faixas etárias
df_faixa = df.withColumn(
    "Faixa_Idade",
    when((col("age") >= 20) & (col("age") <= 25), "20-25")
    .when((col("age") > 25) & (col("age") <= 30), "25-30")
    .when((col("age") > 30) & (col("age") <= 35), "30-35")
    .when((col("age") > 35) & (col("age") <= 40), "35-40")
    .when((col("age") > 40) & (col("age") <= 50), "40-50")
    .otherwise("Acima de 50")
)

# 📊 Calcular média salarial por faixa etária
df_media = df_faixa.groupBy("Faixa_Idade").agg(avg("annual_salary_usd").alias("Media_Salarial")).orderBy("Faixa_Idade")

# 🔎 Converter para Pandas
df_pandas = df_media.toPandas()

# 🎨 Criar gráfico de funil (barras horizontais)
plt.figure(figsize=(8,6))
plt.barh(df_pandas["Faixa_Idade"], df_pandas["Media_Salarial"], color="royalblue", edgecolor="black")

# 🏷️ Personalizar gráfico
plt.title("Funil de Salário Médio por Faixa Etária")
plt.xlabel("Salário Médio (USD)")
plt.ylabel("Faixa de Idade")
plt.grid(axis="x", linestyle="--", alpha=0.7)

# 👀 Exibir gráfico
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico evidencia que a empresa adota uma estrutura salarial baseada em mérito e experiência, e não apenas em títulos acadêmicos. É possível chegar nesta conclusão porque a variação entre o menor e o maior salário é pequena, isso sugere que o peso da educação formal na política salarial da empresa é secundário, enquanto fatores como tempo de experiência, performance e senioridade têm maior impacto.

# COMMAND ----------

from pyspark.sql.functions import avg
import matplotlib.pyplot as plt

# 📥 Leitura do arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Drafts/employee_salary_regression.csv", header=True, inferSchema=True)

# 🔄 Agrupar por nível educacional e calcular média salarial
df_educacao_salario = df.groupBy("education_level").agg(avg("annual_salary_usd").alias("Media_Salarial")).orderBy("education_level")

# 📊 Converter para Pandas
df_pandas = df_educacao_salario.toPandas()

# 🎨 Criar gráfico de barras
plt.figure(figsize=(8,6))
plt.bar(df_pandas["education_level"], df_pandas["Media_Salarial"], color="teal", edgecolor="black")

# 🏷️ Personalizar gráfico
plt.title("Análise Salário x Nível de Educação")
plt.xlabel("Nível de Educação")
plt.ylabel("Salário Médio (USD)")
plt.xticks(rotation=45)
plt.grid(axis="y", linestyle="--", alpha=0.7)

# 👀 Exibir gráfico
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Os dados confirmam que quanto maior a faixa etária, maior é o grau de experiência na função. Essa relação é natural e esperada, mas também estratégica para a empresa: profissionais mais velhos tendem a possuir maior domínio técnico, visão sistêmica e capacidade de tomada de decisão.
# MAGIC O gráfico demonstra que o ganho de experiência acompanha o avanço da idade de forma quase linear, consolidando o perfil de senioridade progressiva dentro da organização.

# COMMAND ----------

from pyspark.sql.functions import when, col, avg
import matplotlib.pyplot as plt

# 📥 Leitura do arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Drafts/employee_salary_regression.csv", header=True, inferSchema=True)

# 🔄 Criar faixas etárias
df_faixa = df.withColumn(
    "Faixa_Idade",
    when((col("age") >= 20) & (col("age") <= 25), "20-25")
    .when((col("age") > 25) & (col("age") <= 30), "25-30")
    .when((col("age") > 30) & (col("age") <= 35), "30-35")
    .when((col("age") > 35) & (col("age") <= 40), "35-40")
    .when((col("age") > 40) & (col("age") <= 50), "40-50")
    .otherwise("Acima de 50")
)

# 📊 Calcular média de anos de experiência por faixa etária
df_media = df_faixa.groupBy("Faixa_Idade").agg(avg("years_experience").alias("Media_Experiencia")).orderBy("Faixa_Idade")

# 🔎 Converter para Pandas
df_pandas = df_media.toPandas()

# 🎨 Criar gráfico de funil (barras horizontais)
plt.figure(figsize=(8,6))
plt.barh(df_pandas["Faixa_Idade"], df_pandas["Media_Experiencia"], color="darkorange", edgecolor="black")

# 🏷️ Personalizar gráfico
plt.title("Funil de Experiência Média por Faixa Etária")
plt.xlabel("Média de Anos de Experiência")
plt.ylabel("Faixa de Idade")
plt.grid(axis="x", linestyle="--", alpha=0.7)

# 👀 Exibir gráfico
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Aproximadamente 52% dos funcionários estão acima dos 40 anos, enquanto 48% estão abaixo dessa faixa.
# MAGIC
# MAGIC Isso representa uma diferença de 4 pontos percentuais a favor das faixas mais maduras, mostrando equilíbrio, mas com leve predominância de profissionais experientes.
# MAGIC
# MAGIC A empresa demonstra uma estrutura etária estável, com distribuição que favorece a retenção de conhecimento e a continuidade operacional.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import when, col
import matplotlib.pyplot as plt

# 📥 Leitura do arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Drafts/employee_salary_regression.csv", header=True, inferSchema=True)

# 🔄 Criar faixas etárias (ranges)
df_range = df.withColumn(
    "Faixa_Idade",
    when((col("age") >= 20) & (col("age") <= 25), "20-25")
    .when((col("age") > 25) & (col("age") <= 30), "25-30")
    .when((col("age") > 30) & (col("age") <= 35), "30-35")
    .when((col("age") > 35) & (col("age") <= 40), "35-40")
    .when((col("age") > 40) & (col("age") <= 50), "40-50")
    .otherwise("Acima de 50")
)

# 📊 Agrupar por faixa etária e contar funcionários
df_faixa = df_range.groupBy("Faixa_Idade").count().orderBy("Faixa_Idade")

# 🔎 Converter para Pandas para visualização
df_pandas = df_faixa.toPandas()

# 🎨 Criar gráfico de barras
plt.figure(figsize=(8,6))
plt.bar(df_pandas["Faixa_Idade"], df_pandas["count"], color="steelblue", edgecolor="black")

# 🏷️ Personalizar o gráfico
plt.title("Distribuição de Funcionários por Faixa Etária")
plt.xlabel("Faixa de Idade")
plt.ylabel("Quantidade de Funcionários")
plt.grid(axis="y", linestyle="--", alpha=0.7)

# 👀 Exibir gráfico
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Para melhora o entendimento, tradução do cabeçalho de inglês para português

# COMMAND ----------

# 📥 Leitura do arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Drafts/employee_salary_regression.csv", header=True, inferSchema=True)

# 🔄 Renomear colunas para português
df_pt = (df
    .withColumnRenamed("employee_id", "ID_Funcionario")
    .withColumnRenamed("age", "Idade")
    .withColumnRenamed("years_experience", "Anos_Experiencia")
    .withColumnRenamed("education_level", "Nivel_Educacao")
    .withColumnRenamed("job_role", "Cargo")
    .withColumnRenamed("city_tier", "Nivel_Cidade")
    .withColumnRenamed("performance_score", "Pontuacao_Desempenho")
    .withColumnRenamed("num_skills", "Qtd_Habilidades")
    .withColumnRenamed("remote_work", "Trabalho_Remoto")
    .withColumnRenamed("annual_salary_usd", "Salario_Anual_USD")
)

# 👀 Visualizar primeiras linhas
display(df_pt.limit(10))

# 📑 Conferir schema traduzido
df_pt.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Importação do arquivo employee_salary_regression.csv

# COMMAND ----------

# 📥 Leitura do novo arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Drafts/employee_salary_regression.csv", header=True, inferSchema=True)

# 👀 Visualizar as primeiras linhas
display(df.limit(10))
