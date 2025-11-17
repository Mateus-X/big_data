from pyspark.sql import functions as F
from pyspark.sql import types as T
import unicodedata
import re

# Caminho do arquivo CSV original do Sisu
sisu_path = "/Volumes/workspace/default/csv/sisu.csv"

# ---------------------------------------------
# Leitura do arquivo CSV
# ---------------------------------------------
sisu = (
    spark.read.option("header", "true")       # Usa a primeira linha como cabeçalho
        .option("inferSchema", "true")        # Faz inferência automática dos tipos
        .csv(sisu_path)                       # Caminho do dataset
)

# ---------------------------------------------
# Escrita da tabela em formato Delta (inserção)
# ---------------------------------------------
sisu.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("delta_sisu")

# ---------------------------------------------
# Filtragem condicional: seleciona somente sexo feminino
# ---------------------------------------------
condicional = sisu.filter(F.col("sexo") == "F")

# Ação: executa a consulta e mostra os resultados
display(condicional)

# ---------------------------------------------
# Busca por texto: cursos contendo "ANÁLISE"
# Coleta todos os cursos com esse termo, agrupados por instituição
# ---------------------------------------------
busca_texto = (
    sisu.filter(F.col("nome_curso").like("%ANÁLISE%"))     # Busca textual no nome do curso
        .groupBy("sigla_ies")                              # Agrupa por instituição
        .agg(F.collect_set("nome_curso").alias("cursos_analise"))  # Lista única de cursos encontrados
)

display(busca_texto)

# ---------------------------------------------
# Cálculo de média de pessoas por curso em cada instituição
# ---------------------------------------------

# 1) Conta quantas pessoas há por curso e por instituição
qtd_por_curso_instituicao = (
    sisu.groupBy("sigla_ies", "nome_curso")
        .agg(F.count("*").alias("total_pessoas"))
)

# 2) Calcula a média de pessoas por curso dentro de cada instituição
pessoas_por_curso = (
    qtd_por_curso_instituicao
        .groupBy("sigla_ies")                                   # Agrupa por instituição
        .agg(F.avg("total_pessoas").alias("media_pessoas_por_curso"))  # Média de alunos por curso
        .orderBy(F.col("media_pessoas_por_curso").desc())       # Ordena da maior média para a menor
)

#mostra o resultado da média
display(pessoas_por_curso)
