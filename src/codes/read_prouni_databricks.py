from pyspark.sql import functions as F
from pyspark.sql import types as T
import unicodedata
import re

prouni_path = "/Volumes/workspace/default/csv/prouni.csv"
sisu_path   = "/Volumes/workspace/default/csv/sisu_verdadero.csv"


# -----------------------------
# Funções auxiliares
# -----------------------------
def remove_accents(s):
    if s is None:
        return None
    return ''.join(
        c for c in unicodedata.normalize('NFKD', s)
        if not unicodedata.combining(c)
    )

def only_digits(s):
    if s is None:
        return None
    return re.sub(r'\D', '', s) 

# -----------------------------
# Leitura dos dados
# -----------------------------
prouni = (
    spark.read.option("header", "true").option("inferSchema", "true")
    .csv(prouni_path)
)
display(prouni)

sisu = (
    spark.read.option("header", "true").option("inferSchema", "true")
    .csv(sisu_path)
)
display(sisu)

prouni.write.format("delta").mode("overwrite").saveAsTable("delta_prouni")
sisu.write.format("delta").mode("overwrite").saveAsTable("delta_sisu")

# -----------------------------
# Salvando tabelas Delta originais
# -----------------------------
prouni.write.format("delta").mode("overwrite").saveAsTable("delta_prouni")
sisu.write.format("delta").mode("overwrite").saveAsTable("delta_sisu")


remove_accents_udf = F.udf(remove_accents)
only_digits_udf = F.udf(only_digits)

prouni_norm = (
    prouni
    .withColumn("curso", F.lower(remove_accents_udf(F.col("curso"))))
    .withColumn("cpf", only_digits_udf(F.col("cpf")))
)
display(prouni_norm)

sisu_norm = (
    sisu
    .withColumn("curso", F.lower(remove_accents_udf(F.col("nome_curso"))))
    .withColumn("cpf", only_digits_udf(F.col("cpf")))
)
display(sisu_norm)

prouni_norm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_prouni")
sisu_norm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_sisu")


# -----------------------------
# Recupera ano mais recente por CPF
# -----------------------------
prouni_ano = (
    prouni_norm
    .filter(F.col("curso").like("%analise e desenvolvimento de sistemas%"))
    .groupBy("cpf")
    .agg(F.max("ano").alias("ano_prouni"))
)

sisu_ano = (
    sisu_norm
    .filter(F.col("curso").like("%analise e desenvolvimento de sistemas%"))
    .groupBy("cpf")
    .agg(F.max("ano").alias("ano_sisu"))
)

# Faz o join e identifica CPFs que entraram no Sisu depois do Prouni
comparativo = (
    prouni_ano.join(sisu_ano, "cpf", "inner")
    .filter(F.col("ano_sisu") > F.col("ano_prouni"))
    .select("cpf", "ano_prouni", "ano_sisu")
    .orderBy("cpf")
)

display(comparativo)

comparativo.write.format("delta").mode("overwrite").saveAsTable("delta_comparativo_cpf")


# -----------------------------
# Consultando delta usando versionamento
# -----------------------------
prouni_delta = spark.read.format("delta").option("versionAsOf", 0).table("delta_prouni")
display(prouni_delta)

sisu_delta = spark.read.format("delta").option("versionAsOf", 0).table("delta_sisu")
display(sisu_delta)


# -----------------------------
# Cálculo de idade
# -----------------------------
latest_prouni = spark.read.format("delta").table("delta_prouni")
latest_sisu   = spark.read.format("delta").table("delta_sisu")

prouni_idade = latest_prouni.withColumn(
    "idade", 
    F.floor(F.months_between(F.current_date(), F.to_date("data_nascimento")) / 12)
)

sisu_idade = latest_sisu.withColumn(
    "idade", 
    F.floor(F.months_between(F.current_date(), F.to_date("data_nascimento")) / 12)
)

prouni_sel = prouni_idade.select("id_ies","curso", "idade")
sisu_sel   = sisu_idade.select("id_ies","curso", "idade")

todas = prouni_sel.unionByName(sisu_sel)

media_idade = (
    todas
    .groupBy("curso", "id_ies")
    .agg(
        F.count(F.col("idade")).alias("total_pessoas"),
        F.round(F.avg(F.col("idade")), 2).alias("media_idade")
    )
    .orderBy("media_idade", ascending=False)
)

display(media_idade)

media_idade.write.format("delta").mode("overwrite").saveAsTable("delta_media_idade_curso")


