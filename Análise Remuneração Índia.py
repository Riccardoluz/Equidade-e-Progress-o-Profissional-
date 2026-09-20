# Databricks notebook source
display(dbutils.fs.ls("/Workspace/Users/rluz07@icloud.com/"))


# COMMAND ----------

from pyspark.sql.functions import avg

# Experiência média por faixa salarial
exp_por_faixa = df_pt.groupBy("Faixa_Salarial").agg(avg("Experiencia_Dominio").alias("Experiencia_Media")).toPandas()

# Idade média por faixa salarial
idade_por_faixa = df_pt.groupBy("Faixa_Salarial").agg(avg("Idade").alias("Idade_Media")).toPandas()

# Taxa de saída por gênero
saida_por_genero = df_pt.groupBy("Genero").agg(
    (avg(when(df_pt["Saiu_ou_Nao"] == "Sim", 1).otherwise(0))*100).alias("Taxa_Saida")
).toPandas()

# Conclusões automáticas
print("📌 Conclusões Automáticas do MVP\n")

# Experiência
faixa1 = exp_por_faixa.loc[exp_por_faixa["Faixa_Salarial"]=="Faixa 1","Experiencia_Media"].values[0]
faixa3 = exp_por_faixa.loc[exp_por_faixa["Faixa_Salarial"]=="Faixa 3","Experiencia_Media"].values[0]
print(f"Funcionários da Faixa 3 têm em média {faixa3-faixa1:.1f} anos a mais de experiência que os da Faixa 1.")

# Idade
idade1 = idade_por_faixa.loc[idade_por_faixa["Faixa_Salarial"]=="Faixa 1","Idade_Media"].values[0]
idade3 = idade_por_faixa.loc[idade_por_faixa["Faixa_Salarial"]=="Faixa 3","Idade_Media"].values[0]
print(f"A idade média na Faixa 3 é {idade3-idade1:.1f} anos superior à da Faixa 1.")

# Saída por gênero
for i, row in saida_por_genero.iterrows():
    print(f"A taxa de saída entre {row['Genero']} é de {row['Taxa_Saida']:.1f}%.")

print("\n✅ Essas conclusões sintetizam os principais padrões encontrados no dataset.")


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico confirma que a idade acompanha o tempo de experiência até os 30 anos, mas a partir daí o crescimento se torna mais lento e homogêneo entre as faixas superiores.
# MAGIC Esse padrão é típico de empresas com estrutura de cargos bem definida, onde o tempo de atuação deixa de ser o principal fator de diferenciação após a consolidação profissional.

# COMMAND ----------

from pyspark.sql.functions import when, col, avg
import matplotlib.pyplot as plt

# 📥 Leitura do arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Employee.csv", header=True, inferSchema=True)

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
df_media = df_faixa.groupBy("Faixa_Idade").agg(avg("ExperienceInCurrentDomain").alias("Media_Experiencia")).orderBy("Faixa_Idade")

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
# MAGIC A análise indica que mudanças de função exigem atenção estratégica. Embora a mobilidade interna seja positiva para o desenvolvimento profissional, ela também pode aumentar o risco de saída se não houver planejamento de carreira, acompanhamento e adaptação ao novo papel.
# MAGIC
# MAGIC Em resumo, os dados mostram que a experiência de desalocação (mudança de função) está associada a uma maior probabilidade de desligamento, reforçando a importância de políticas de gestão de transição e engajamento para garantir que essas movimentações resultem em crescimento, e não em evasão de talentos.

# COMMAND ----------

df_pandas = df_pt.select("Ja_Desalocado", "Saiu_ou_Nao").toPandas()
plt.figure(figsize=(8,5))
sns.countplot(data=df_pandas, x="Ja_Desalocado", hue="Saiu_ou_Nao", palette="magma")
plt.title("Desalocação vs Saída da Empresa", fontsize=14, fontweight="bold")
plt.xlabel("Já ficou desalocado")
plt.ylabel("Quantidade")
plt.legend(title="Saiu da Empresa", labels=["Não", "Sim"])
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O comportamento dos dados reforça que a experiência cresce com a idade, mas a trajetória profissional é multifacetada, e o tempo de casa ou de domínio pode variar conforme as oportunidades de desenvolvimento e mudança de carreira.

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# Carregar df_pt da tabela Delta se não estiver definido
if 'df_pt' not in locals():
    df_pt = spark.read.table("silver.employee_pt")

# Converter para Pandas
df_pandas = df_pt.select("Idade", "Experiencia_Dominio").toPandas()

plt.figure(figsize=(8,5))
sns.regplot(data=df_pandas, x="Idade", y="Experiencia_Dominio", color="purple", scatter_kws={'alpha':0.5})
plt.title("Correlação entre Idade e Experiência Profissional", fontsize=14, fontweight="bold")
plt.xlabel("Idade")
plt.ylabel("Experiência (anos)")
plt.grid(True, linestyle="--", alpha=0.6)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico revela que, embora a empresa mantenha uma boa taxa de retenção geral, há uma tendência maior de saída entre mulheres.
# MAGIC Esse comportamento pode estar relacionado a fatores como menor satisfação com o ambiente de trabalho, diferenças de progressão de carreira ou desafios de conciliação familiar.
# MAGIC
# MAGIC Em termos estratégicos, a empresa poderia investigar mais a fundo as causas dessa diferença e adotar políticas de equidade de gênero, flexibilidade de jornada e programas de desenvolvimento profissional voltados para mulheres, fortalecendo a retenção e a diversidade.

# COMMAND ----------

df_pandas = df_pt.select("Genero", "Saiu_ou_Nao").toPandas()
plt.figure(figsize=(8,5))
sns.countplot(data=df_pandas, x="Genero", hue="Saiu_ou_Nao", palette="Set2")
plt.title("Taxa de Saída por Gênero", fontsize=14, fontweight="bold")
plt.xlabel("Gênero")
plt.ylabel("Quantidade")
plt.legend(title="Saiu da Empresa", labels=["Não", "Sim"])
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico confirma que o nível de experiência é determinante para o posicionamento na faixa salarial, mas não de forma isolada.
# MAGIC A semelhança entre as distribuições indica que a empresa valoriza a experiência, porém mantém equilíbrio entre faixas, permitindo que profissionais com menor tempo de atuação também alcancem boas remunerações possivelmente por mérito, especialização ou desempenho.

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

df_pandas = df_pt.select("Faixa_Salarial", "Experiencia_Dominio").toPandas()
plt.figure(figsize=(8,5))
sns.boxplot(data=df_pandas, x="Faixa_Salarial", y="Experiencia_Dominio", palette="viridis")
plt.title("Experiência por Faixa Salarial", fontsize=14, fontweight="bold")
plt.xlabel("Faixa Salarial")
plt.ylabel("Experiência (anos)")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico evidencia que Bangalore é o núcleo de maior concentração de talentos e remuneração elevada, refletindo um ambiente corporativo mais maduro e competitivo.
# MAGIC Já Pune e New Delhi apresentam perfis mais equilibrados, com predominância de faixas intermediárias, o que pode indicar centros de suporte ou operações em expansão.
# MAGIC
# MAGIC Em síntese, os dados reforçam que a localização geográfica influencia diretamente o perfil salarial, e Bangalore se consolida como o principal polo estratégico da empresa em termos de experiência e remuneração.

# COMMAND ----------

df_pandas = df_pt.select("Cidade", "Faixa_Salarial").toPandas()

plt.figure(figsize=(9,5))
sns.countplot(data=df_pandas, x="Cidade", hue="Faixa_Salarial", palette="coolwarm")
plt.title("Distribuição de Faixas Salariais por Cidade", fontsize=14, fontweight="bold")
plt.xlabel("Cidade", fontsize=12)
plt.ylabel("Quantidade de Funcionários", fontsize=12)
plt.legend(title="Faixa Salarial")
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Os dados mostram uma distribuição assimétrica, onde Bangalore lidera amplamente, seguida por Pune e New Delhi o que sugere uma estratégia de expansão regional equilibrada, mas com foco principal na cidade de Bangalore.

# COMMAND ----------

df_pandas = df_pt.select("Cidade").toPandas()
plt.figure(figsize=(8,5))
df_pandas["Cidade"].value_counts().plot(kind="barh", color="orange", edgecolor="black")

plt.title("Distribuição de Funcionários por Cidade", fontsize=14, fontweight="bold")
plt.xlabel("Quantidade de Funcionários", fontsize=12)
plt.ylabel("Cidade", fontsize=12)
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico evidencia que a representatividade feminina diminui nas faixas salariais mais altas, o que pode refletir barreiras de progressão de carreira ou menor presença de mulheres em cargos de liderança.
# MAGIC Por outro lado, a maior presença feminina nas faixas intermediárias mostra que há um bom potencial de crescimento, desde que existam políticas de equidade e desenvolvimento profissional.

# COMMAND ----------

import seaborn as sns

df_pandas = df_pt.select("Genero", "Faixa_Salarial").toPandas()

sns.countplot(data=df_pandas, x="Faixa_Salarial", hue="Genero")
plt.title("Distribuição de Gênero por Faixa Salarial")
plt.xlabel("Faixa Salarial")
plt.ylabel("Quantidade")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico evidencia que a educação é um fator determinante para o posicionamento salarial, mas não o único.
# MAGIC A predominância de graduados na faixa mais alta sugere que experiência e desempenho também desempenham papel relevante na progressão salarial.
# MAGIC Já os colaboradores com PhD confirmam a tendência de que níveis acadêmicos mais elevados garantem acesso às faixas salariais superiores, ainda que representem uma minoria dentro da empresa.

# COMMAND ----------

from pyspark.sql.functions import col, count
import matplotlib.pyplot as plt

# Carregar df_pt da tabela Delta se não estiver definido
if 'df_pt' not in locals():
    df_pt = spark.read.table("silver.employee_pt")

# 🔄 Agrupar por Educação e Faixa Salarial
df_educacao_salario = df_pt.groupBy("Educacao", "Faixa_Salarial").agg(count("*").alias("Qtd_Funcionarios"))

# 📊 Converter para Pandas
df_pandas = df_educacao_salario.toPandas()

# 🎨 Criar gráfico de barras agrupadas
plt.figure(figsize=(10,6))
for faixa in df_pandas["Faixa_Salarial"].unique():
    subset = df_pandas[df_pandas["Faixa_Salarial"] == faixa]
    plt.bar(subset["Educacao"], subset["Qtd_Funcionarios"], label=faixa)

# 🏷️ Personalizar gráfico
plt.title("Comparação Educação x Faixa Salarial")
plt.xlabel("Nível de Educação")
plt.ylabel("Quantidade de Funcionários")
plt.xticks(rotation=45)
plt.legend(title="Faixa Salarial")
plt.grid(axis="y", linestyle="--", alpha=0.7)

# 👀 Exibir gráfico
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico evidencia que a empresa possui um perfil jovem e dinâmico, o que pode favorecer inovação, adaptabilidade e energia no ambiente de trabalho.
# MAGIC Por outro lado, a menor presença de profissionais mais velhos pode indicar desafios na retenção de talentos experientes ou falta de equilíbrio geracional — algo que pode ser estratégico revisar para fortalecer a transferência de conhecimento e a mentoria interna.

# COMMAND ----------

import matplotlib.pyplot as plt

# Converter para Pandas para visualização
df_pandas = df_pt.select("Idade").toPandas()

plt.hist(df_pandas["Idade"], bins=10, color="skyblue", edgecolor="black")
plt.title("Distribuição de Idade dos Funcionários")
plt.xlabel("Idade")
plt.ylabel("Quantidade")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Está etapa estou realizando a aanálise da qualidadde dos dados.

# COMMAND ----------

# Valores nulos por coluna
for col in df_pt.columns:
    print(col, df_pt.filter(df_pt[col].isNull()).count())

# Duplicatas
print("Duplicatas:", df_pt.count() - df_pt.dropDuplicates().count())

# Estatísticas básicas
df_pt.describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC criação de tabelas Gold, agregadas e prontas para análise de negócio:

# COMMAND ----------

# Criar schema se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Salário médio por cidade e faixa salarial
df_gold_salario = df_pt.groupBy("Cidade", "Faixa_Salarial") \
    .agg({"Idade":"avg", "Experiencia_Dominio":"avg"}) \
    .withColumnRenamed("avg(Idade)", "Idade_Media") \
    .withColumnRenamed("avg(Experiencia_Dominio)", "Experiencia_Media")

df_gold_salario.write.format("delta").mode("overwrite").saveAsTable("gold.salario_por_cidade")

# Diversidade de gênero por faixa salarial
df_gold_genero = df_pt.groupBy("Faixa_Salarial", "Genero") \
    .count() \
    .withColumnRenamed("count", "Total")

df_gold_genero.write.format("delta").mode("overwrite").saveAsTable("gold.diversidade_genero")


# COMMAND ----------

# MAGIC %md
# MAGIC Este foi o script utilizado para realizar a tradução do nome das colunas, com este metodo pretendo facilitar a identificação das colunas.

# COMMAND ----------

from pyspark.sql.functions import when

# Leitura do CSV original
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Employee.csv", header=True, inferSchema=True)

# Renomear colunas para português
df_pt = df.withColumnRenamed("Education", "Educacao") \
          .withColumnRenamed("JoiningYear", "Ano_Ingresso") \
          .withColumnRenamed("City", "Cidade") \
          .withColumnRenamed("PaymentTier", "Faixa_Salarial") \
          .withColumnRenamed("Age", "Idade") \
          .withColumnRenamed("Gender", "Genero") \
          .withColumnRenamed("EverBenched", "Ja_Desalocado") \
          .withColumnRenamed("ExperienceInCurrentDomain", "Experiencia_Dominio") \
          .withColumnRenamed("LeaveOrNot", "Saiu_ou_Nao")

# Traduzir valores categóricos
df_pt = df_pt.withColumn("Genero",
    when(df_pt["Genero"] == "Male", "Masculino")
    .when(df_pt["Genero"] == "Female", "Feminino")
    .otherwise(df_pt["Genero"])
)

df_pt = df_pt.withColumn("Faixa_Salarial",
    when(df_pt["Faixa_Salarial"] == 1, "Faixa 1")
    .when(df_pt["Faixa_Salarial"] == 2, "Faixa 2")
    .when(df_pt["Faixa_Salarial"] == 3, "Faixa 3")
    .otherwise(df_pt["Faixa_Salarial"].cast("string"))
)

df_pt = df_pt.withColumn("Ja_Desalocado",
    when(df_pt["Ja_Desalocado"] == "Yes", "Sim")
    .when(df_pt["Ja_Desalocado"] == "No", "Não")
    .otherwise(df_pt["Ja_Desalocado"])
)

df_pt = df_pt.withColumn("Saiu_ou_Nao",
    when(df_pt["Saiu_ou_Nao"] == 1, "Sim")
    .when(df_pt["Saiu_ou_Nao"] == 0, "Não")
    .otherwise(df_pt["Saiu_ou_Nao"].cast("string"))
)

# Visualizar primeiras linhas já traduzidas
display(df_pt.limit(10))

# Criar schema se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Salvar como tabela Silver
df_pt.write.format("delta").mode("overwrite").saveAsTable("silver.employee_pt")


# COMMAND ----------

# MAGIC %md
# MAGIC Importação do Dataset (Emploee.csv) tem como objetivo de analisar a igualdade salarial entre gênero, através da análise entre a variação de remunueração por cidades, gênero, nível de experiência e nível de Graduação.

# COMMAND ----------

# Leitura do arquivo CSV
df = spark.read.csv("/Workspace/Users/rluz07@icloud.com/Employee.csv", header=True, inferSchema=True)

# Visualizar as primeiras linhas
display(df.limit(10))
