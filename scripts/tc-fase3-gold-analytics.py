import sys

from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. INICIALIZACAO AWS GLUE / SPARK
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. CAMINHOS DO DATA LAKE
# ============================================================

BUCKET = "fiap-techchallenge-fase3-221507154727"

SILVER_RESPONDENTES = (
    f"s3://{BUCKET}/silver/respondentes/"
)

SILVER_TECNOLOGIAS = (
    f"s3://{BUCKET}/silver/tecnologias/"
)

SILVER_IA = (
    f"s3://{BUCKET}/silver/ia/"
)


GOLD_PERFIL = (
    f"s3://{BUCKET}/gold/perfil_mercado/"
)

GOLD_REMUNERACAO = (
    f"s3://{BUCKET}/gold/remuneracao/"
)

GOLD_TECNOLOGIAS = (
    f"s3://{BUCKET}/gold/tecnologias/"
)

GOLD_IA = (
    f"s3://{BUCKET}/gold/ia/"
)


# ============================================================
# 3. LEITURA DAS CAMADAS SILVER
# ============================================================

print("Lendo silver_respondentes...")

respondentes = spark.read.parquet(
    SILVER_RESPONDENTES
)


print("Lendo silver_tecnologias...")

tecnologias = spark.read.parquet(
    SILVER_TECNOLOGIAS
)


print("Lendo silver_ia...")

ia = spark.read.parquet(
    SILVER_IA
)


# ============================================================
# 4. GOLD - PERFIL DO MERCADO
#
# Estrutura:
# ano
# dimensao
# categoria
# quantidade
# total_validos
# percentual
# ranking
#
# Cada dimensao possui seu proprio denominador.
# ============================================================

DIMENSOES_PERFIL = [
    "senioridade",
    "genero",
    "raca_etnia",
    "pcd",
    "regiao",
    "cargo_atual",
    "modelo_trabalho_atual",
    "nivel_ensino",
    "situacao_trabalho"
]


def criar_dimensao_perfil(df, dimensao):

    base = (
        df
        .filter(
            F.col(dimensao).isNotNull()
            & (
                F.trim(
                    F.col(dimensao)
                ) != ""
            )
        )
    )

    contagem = (
        base
        .groupBy(
            "ano_pesquisa",
            dimensao
        )
        .agg(
            F.countDistinct(
                "id_respondente"
            ).alias("quantidade")
        )
        .withColumnRenamed(
            dimensao,
            "categoria"
        )
    )

    totais = (
        base
        .groupBy(
            "ano_pesquisa"
        )
        .agg(
            F.countDistinct(
                "id_respondente"
            ).alias("total_validos")
        )
    )

    resultado = (
        contagem

        .join(
            totais,
            on="ano_pesquisa",
            how="left"
        )

        .withColumn(
            "dimensao",
            F.lit(dimensao)
        )

        .withColumn(
            "percentual",
            F.round(
                (
                    F.col("quantidade")
                    * F.lit(100.0)
                )
                / F.col("total_validos"),
                2
            )
        )

        .select(
            "ano_pesquisa",
            "dimensao",
            "categoria",
            "quantidade",
            "total_validos",
            "percentual"
        )
    )

    return resultado


perfil_partes = [
    criar_dimensao_perfil(
        respondentes,
        dimensao
    )
    for dimensao
    in DIMENSOES_PERFIL
]


gold_perfil = reduce(
    lambda a, b:
        a.unionByName(b),
    perfil_partes
)


window_perfil = Window.partitionBy(
    "ano_pesquisa",
    "dimensao"
).orderBy(
    F.desc("quantidade"),
    F.asc("categoria")
)


gold_perfil = (
    gold_perfil
    .withColumn(
        "ranking",
        F.row_number().over(
            window_perfil
        )
    )
)


# ============================================================
# 5. GOLD - REMUNERACAO
#
# Distribuicao de faixa salarial:
#
# - geral
# - por senioridade
# - por genero
# - por regiao
# - por cargo
#
# A faixa original e preservada.
# ============================================================

def base_salario(df):

    return (
        df
        .filter(
            F.col(
                "faixa_salarial_original"
            ).isNotNull()
            & (
                F.trim(
                    F.col(
                        "faixa_salarial_original"
                    )
                ) != ""
            )
        )
    )


def criar_remuneracao_geral(df):

    base = base_salario(df)

    contagem = (
        base
        .groupBy(
            "ano_pesquisa",
            "faixa_salarial_original"
        )
        .agg(
            F.countDistinct(
                "id_respondente"
            ).alias("quantidade")
        )

        .withColumn(
            "dimensao_analise",
            F.lit("geral")
        )

        .withColumn(
            "categoria_analise",
            F.lit("todos")
        )
    )


    totais = (
        base
        .groupBy(
            "ano_pesquisa"
        )
        .agg(
            F.countDistinct(
                "id_respondente"
            ).alias("total_validos")
        )
    )


    return (
        contagem
        .join(
            totais,
            on="ano_pesquisa",
            how="left"
        )
    )


def criar_remuneracao_dimensao(
    df,
    dimensao
):

    base = (
        base_salario(df)
        .filter(
            F.col(dimensao).isNotNull()
            & (
                F.trim(
                    F.col(dimensao)
                ) != ""
            )
        )
    )


    contagem = (
        base
        .groupBy(
            "ano_pesquisa",
            dimensao,
            "faixa_salarial_original"
        )
        .agg(
            F.countDistinct(
                "id_respondente"
            ).alias("quantidade")
        )

        .withColumnRenamed(
            dimensao,
            "categoria_analise"
        )

        .withColumn(
            "dimensao_analise",
            F.lit(dimensao)
        )
    )


    totais = (
        base
        .groupBy(
            "ano_pesquisa",
            dimensao
        )
        .agg(
            F.countDistinct(
                "id_respondente"
            ).alias("total_validos")
        )

        .withColumnRenamed(
            dimensao,
            "categoria_analise"
        )
    )


    return (
        contagem
        .join(
            totais,
            on=[
                "ano_pesquisa",
                "categoria_analise"
            ],
            how="left"
        )
    )


remuneracao_partes = [
    criar_remuneracao_geral(
        respondentes
    )
]


for dimensao in [
    "senioridade",
    "genero",
    "regiao",
    "cargo_atual"
]:

    remuneracao_partes.append(
        criar_remuneracao_dimensao(
            respondentes,
            dimensao
        )
    )


gold_remuneracao = reduce(
    lambda a, b:
        a.unionByName(
            b,
            allowMissingColumns=True
        ),
    remuneracao_partes
)


gold_remuneracao = (
    gold_remuneracao

    .withColumn(
        "percentual",
        F.round(
            (
                F.col("quantidade")
                * F.lit(100.0)
            )
            / F.col("total_validos"),
            2
        )
    )
)


# ============================================================
# 5.1 ORDENACAO DAS FAIXAS SALARIAIS
#
# Extraimos o primeiro valor monetario da faixa.
#
# Exemplo:
# "de R$ 8.001/mês a R$ 12.000/mês"
# -> 8001
#
# O texto original NAO e alterado.
# ============================================================

primeiro_valor = (
    F.regexp_replace(
        F.regexp_extract(
            F.col(
                "faixa_salarial_original"
            ),
            r"R\$\s*([0-9\.]+)",
            1
        ),
        r"\.",
        ""
    )
    .cast("int")
)


# Anomalias identificadas na fonte bruta.
# Mantemos o valor original e apenas sinalizamos.

anomalia_salario = (
    F.col(
        "faixa_salarial_original"
    ).startswith(
        "de R$ 101/"
    )

    |

    F.col(
        "faixa_salarial_original"
    ).contains(
        "R$ 3000/"
    )
)


gold_remuneracao = (
    gold_remuneracao

    .withColumn(
        "anomalia_faixa_salarial",
        anomalia_salario
    )

    .withColumn(
        "faixa_salarial_ordem",

        F.when(
            F.col(
                "faixa_salarial_original"
            ).startswith(
                "de R$ 101/"
            ),
            F.lit(1001)
        )

        .otherwise(
            primeiro_valor
        )
    )
)


gold_remuneracao = (
    gold_remuneracao
    .select(
        "ano_pesquisa",
        "dimensao_analise",
        "categoria_analise",
        "faixa_salarial_original",
        "faixa_salarial_ordem",
        "anomalia_faixa_salarial",
        "quantidade",
        "total_validos",
        "percentual"
    )
)


# ============================================================
# 6. GOLD - TECNOLOGIAS
#
# Numerador:
# profissionais que selecionaram a tecnologia.
#
# Denominador:
# respondentes validos daquela categoria/ano.
#
# Exemplo:
# banco_dados / 2024
# ============================================================

respondentes_categoria = (
    tecnologias

    .groupBy(
        "ano_pesquisa",
        "categoria"
    )

    .agg(
        F.countDistinct(
            "id_respondente"
        ).alias(
            "total_respondentes_categoria"
        )
    )
)


adocao_tecnologias = (
    tecnologias

    .filter(
        F.col(
            "tipo_item"
        ) == "tecnologia"
    )

    .groupBy(
        "ano_pesquisa",
        "categoria",
        "tecnologia",
        "comparabilidade"
    )

    .agg(
        F.countDistinct(
            "id_respondente"
        ).alias(
            "qtd_profissionais"
        )
    )
)


gold_tecnologias = (
    adocao_tecnologias

    .join(
        respondentes_categoria,
        on=[
            "ano_pesquisa",
            "categoria"
        ],
        how="left"
    )

    .withColumn(
        "percentual_adocao",
        F.round(
            (
                F.col(
                    "qtd_profissionais"
                )
                * F.lit(100.0)
            )
            /
            F.col(
                "total_respondentes_categoria"
            ),
            2
        )
    )
)


window_tech = Window.partitionBy(
    "ano_pesquisa",
    "categoria"
).orderBy(
    F.desc(
        "qtd_profissionais"
    ),
    F.asc(
        "tecnologia"
    )
)


gold_tecnologias = (
    gold_tecnologias

    .withColumn(
        "ranking",
        F.row_number().over(
            window_tech
        )
    )

    .select(
        "ano_pesquisa",
        "categoria",
        "tecnologia",
        "qtd_profissionais",
        "total_respondentes_categoria",
        "percentual_adocao",
        "ranking",
        "comparabilidade"
    )
)


# ============================================================
# 7. GOLD - INTELIGENCIA ARTIFICIAL
#
# Cada dimensao usa como denominador apenas
# os respondentes validos daquela pergunta.
#
# Multisselecao pode somar mais de 100%.
# ============================================================

totais_ia = (
    ia

    .groupBy(
        "ano_pesquisa",
        "dimensao"
    )

    .agg(
        F.countDistinct(
            "id_respondente"
        ).alias(
            "total_respondentes_dimensao"
        )
    )
)


contagem_ia = (
    ia

    .groupBy(
        "ano_pesquisa",
        "dimensao",
        "resposta",
        "tipo_resposta",
        "comparabilidade"
    )

    .agg(
        F.countDistinct(
            "id_respondente"
        ).alias(
            "qtd_respondentes"
        )
    )
)


gold_ia = (
    contagem_ia

    .join(
        totais_ia,
        on=[
            "ano_pesquisa",
            "dimensao"
        ],
        how="left"
    )

    .withColumn(
        "percentual",
        F.round(
            (
                F.col(
                    "qtd_respondentes"
                )
                * F.lit(100.0)
            )
            /
            F.col(
                "total_respondentes_dimensao"
            ),
            2
        )
    )

    .withColumn(
        "percentuais_somam_100",

        F.when(
            F.col(
                "tipo_resposta"
            ) == "categorica",
            F.lit(True)
        )

        .otherwise(
            F.lit(False)
        )
    )
)


window_ia = Window.partitionBy(
    "ano_pesquisa",
    "dimensao"
).orderBy(
    F.desc(
        "qtd_respondentes"
    ),
    F.asc(
        "resposta"
    )
)


gold_ia = (
    gold_ia

    .withColumn(
        "ranking",
        F.row_number().over(
            window_ia
        )
    )

    .select(
        "ano_pesquisa",
        "dimensao",
        "resposta",
        "qtd_respondentes",
        "total_respondentes_dimensao",
        "percentual",
        "ranking",
        "tipo_resposta",
        "percentuais_somam_100",
        "comparabilidade"
    )
)


# ============================================================
# 8. VALIDACOES
# ============================================================

print(
    "=========================================="
)

print(
    "VALIDACOES GOLD"
)

print(
    "=========================================="
)


print(
    f"Gold perfil: "
    f"{gold_perfil.count()} linhas"
)


print(
    f"Gold remuneracao: "
    f"{gold_remuneracao.count()} linhas"
)


print(
    f"Gold tecnologias: "
    f"{gold_tecnologias.count()} linhas"
)


print(
    f"Gold IA: "
    f"{gold_ia.count()} linhas"
)


print(
    "Top senioridade:"
)

(
    gold_perfil
    .filter(
        F.col(
            "dimensao"
        ) == "senioridade"
    )
    .orderBy(
        "ano_pesquisa",
        "ranking"
    )
    .show(
        20,
        truncate=False
    )
)


print(
    "Top tecnologias:"
)

(
    gold_tecnologias
    .filter(
        F.col(
            "ranking"
        ) <= 5
    )
    .orderBy(
        "ano_pesquisa",
        "categoria",
        "ranking"
    )
    .show(
        100,
        truncate=False
    )
)


print(
    "Prioridade de IA:"
)

(
    gold_ia
    .filter(
        F.col(
            "dimensao"
        ) == "prioridade_empresa"
    )
    .orderBy(
        "ano_pesquisa",
        "ranking"
    )
    .show(
        30,
        truncate=False
    )
)


# ============================================================
# 9. GRAVACAO GOLD - PERFIL
# ============================================================

(
    gold_perfil

    .repartition(
        3,
        "ano_pesquisa"
    )

    .write

    .mode(
        "overwrite"
    )

    .format(
        "parquet"
    )

    .partitionBy(
        "ano_pesquisa"
    )

    .save(
        GOLD_PERFIL
    )
)


# ============================================================
# 10. GRAVACAO GOLD - REMUNERACAO
# ============================================================

(
    gold_remuneracao

    .repartition(
        3,
        "ano_pesquisa"
    )

    .write

    .mode(
        "overwrite"
    )

    .format(
        "parquet"
    )

    .partitionBy(
        "ano_pesquisa"
    )

    .save(
        GOLD_REMUNERACAO
    )
)


# ============================================================
# 11. GRAVACAO GOLD - TECNOLOGIAS
# ============================================================

(
    gold_tecnologias

    .repartition(
        3,
        "ano_pesquisa"
    )

    .write

    .mode(
        "overwrite"
    )

    .format(
        "parquet"
    )

    .partitionBy(
        "ano_pesquisa"
    )

    .save(
        GOLD_TECNOLOGIAS
    )
)


# ============================================================
# 12. GRAVACAO GOLD - IA
# ============================================================

(
    gold_ia

    .repartition(
        3,
        "ano_pesquisa"
    )

    .write

    .mode(
        "overwrite"
    )

    .format(
        "parquet"
    )

    .partitionBy(
        "ano_pesquisa"
    )

    .save(
        GOLD_IA
    )
)


print(
    "=========================================="
)

print(
    "CAMADA GOLD GERADA COM SUCESSO"
)

print(
    "=========================================="
)


print(
    GOLD_PERFIL
)

print(
    GOLD_REMUNERACAO
)

print(
    GOLD_TECNOLOGIAS
)

print(
    GOLD_IA
)


# ============================================================
# 13. FINALIZACAO
# ============================================================

job.commit()