import sys
import ast

from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


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

PATH_2023 = f"s3://{BUCKET}/bronze/state_of_data/ano=2023/"
PATH_2024 = f"s3://{BUCKET}/bronze/state_of_data/ano=2024/"
PATH_2025 = f"s3://{BUCKET}/bronze/state_of_data/ano=2025/"

SILVER_PATH = f"s3://{BUCKET}/silver/tecnologias/"


# ============================================================
# 3. CONFIGURACAO DOS CAMPOS DE CADA ANO
#
# Os codigos das perguntas mudaram entre as pesquisas.
# Em vez de listar dezenas de tecnologias manualmente,
# identificamos as colunas pelo prefixo correspondente.
# ============================================================

CONFIG = {

    2023: {
        "id": "('P0', 'id')",

        "prefixos": {
            "linguagem": "('P4_d_",
            "banco_dados": "('P4_g_",
            "cloud": "('P4_h_",
            "bi": "('P4_j_"
        }
    },

    2024: {
        "id": "0.a_token",

        "prefixos": {
            "linguagem": "4.d.",
            "banco_dados": "4.g.",
            "cloud": "4.h.",
            "bi": "4.j."
        }
    },

    2025: {
        "id": "0.a_token",

        "prefixos": {
            "linguagem": "4.c.",
            "banco_dados": "4.d.",
            "cloud": "4.e.",
            "bi": "4.g."
        }
    }
}


# ============================================================
# 4. PERGUNTA DE ORIGEM
#
# Guardamos a semantica da pergunta original porque algumas
# perguntas sofreram alteracoes entre os anos.
# ============================================================

PERGUNTAS = {

    2023: {
        "linguagem":
            "Quais das linguagens listadas abaixo voce utiliza no trabalho?",

        "banco_dados":
            "Quais dos bancos de dados/fontes de dados listados abaixo voce utiliza no trabalho?",

        "cloud":
            "Dentre as opcoes listadas, qual sua Cloud preferida?",

        "bi":
            "Ferramenta de BI utilizada no dia a dia"
    },

    2024: {
        "linguagem":
            "linguagem_de_programacao_(dia_a_dia)",

        "banco_dados":
            "banco_de_dados_(dia_a_dia)",

        "cloud":
            "cloud_(dia_a_dia)",

        "bi":
            "ferramenta_de_bi_(dia_a_dia)"
    },

    2025: {
        "linguagem":
            "linguagem_preferida",

        "banco_dados":
            "banco_de_dados_(dia_a_dia)",

        "cloud":
            "cloud_(dia_a_dia)",

        "bi":
            "ferramenta_de_bi_(dia_a_dia)"
    }
}


# ============================================================
# 5. COMPARABILIDADE ENTRE OS ANOS
#
# 2025 mudou a denominacao da pergunta de linguagens.
# 2023 possui uma particularidade no rotulo da pergunta Cloud.
# Preservamos isso para nao gerar conclusoes enganosas depois.
# ============================================================

COMPARABILIDADE = {

    2023: {
        "linguagem": "comparavel",
        "banco_dados": "comparavel",
        "cloud": "atencao_semantica",
        "bi": "comparavel"
    },

    2024: {
        "linguagem": "comparavel",
        "banco_dados": "comparavel",
        "cloud": "comparavel",
        "bi": "comparavel"
    },

    2025: {
        "linguagem": "atencao_semantica",
        "banco_dados": "comparavel",
        "cloud": "comparavel",
        "bi": "comparavel"
    }
}


# ============================================================
# 6. FUNCOES AUXILIARES
# ============================================================

def ler_csv(caminho):
    """
    Le os CSVs da Bronze sem inferir schema.
    Assim preservamos os dados de origem antes das transformacoes.
    """

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .csv(caminho)
    )


def coluna(nome):
    """
    Permite acessar colunas que possuem pontos,
    parenteses, barras e outros caracteres especiais.
    """

    nome_seguro = nome.replace("`", "``")
    return F.col(f"`{nome_seguro}`")


def extrair_rotulo_coluna(nome_coluna, ano):
    """
    Extrai apenas o nome da tecnologia a partir
    do cabecalho original.
    """

    if ano == 2023:

        try:
            valor = ast.literal_eval(nome_coluna)

            if isinstance(valor, tuple) and len(valor) >= 2:
                return str(valor[1]).strip()

        except Exception:
            return nome_coluna.strip()

    # 2024 e 2025:
    # Exemplo: 4.d.3_Python -> Python

    if "_" in nome_coluna:
        return nome_coluna.split("_", 1)[1].strip()

    return nome_coluna.strip()


def normalizar_tecnologia(rotulo):
    """
    Pequenas harmonizacoes de nomenclatura.
    O valor original tambem sera mantido na Silver.
    """

    aliases = {
        "SQL SERVER": "SQL Server",
        "ElasticSearch": "Elasticsearch",
        "Neo4J": "Neo4j",
        "Amazon Quicksight": "Amazon QuickSight",
        "Looker Studio(Google Data Studio)":
            "Looker Studio (Google Data Studio)"
    }

    rotulo = rotulo.strip()

    return aliases.get(rotulo, rotulo)


def classificar_item(rotulo):
    """
    Diferencia tecnologias propriamente ditas de respostas
    como 'nao utilizo' ou uso apenas de planilhas.
    """

    texto = rotulo.lower()

    if "não utilizo" in texto or "nao utilizo" in texto:
        return "nao_utiliza"

    if "servidores on premise" in texto:
        return "nao_cloud"

    if "análises utilizando apenas excel" in texto:
        return "alternativa_planilhas"

    if "analises utilizando apenas excel" in texto:
        return "alternativa_planilhas"

    return "tecnologia"


# ============================================================
# 7. TRANSFORMACAO WIDE -> LONG
#
# Exemplo de origem:
#
# pessoa | SQL | Python | PowerBI
# A      |  1  |   1    |   0
#
# Resultado:
#
# A | linguagem | SQL
# A | linguagem | Python
# ============================================================

def transformar_tecnologias(df, ano):

    config = CONFIG[ano]
    id_col = config["id"]

    dataframes_categorias = []

    for categoria, prefixo in config["prefixos"].items():

        colunas_tecnologia = [
            c for c in df.columns
            if c.startswith(prefixo)
        ]

        if len(colunas_tecnologia) == 0:
            raise ValueError(
                f"Nenhuma coluna encontrada para "
                f"ano={ano}, categoria={categoria}, prefixo={prefixo}"
            )

        print(
            f"{ano} - {categoria}: "
            f"{len(colunas_tecnologia)} colunas encontradas"
        )

        itens = []

        for nome_coluna in colunas_tecnologia:

            rotulo_original = extrair_rotulo_coluna(
                nome_coluna,
                ano
            )

            tecnologia = normalizar_tecnologia(
                rotulo_original
            )

            tipo_item = classificar_item(
                rotulo_original
            )

            # Os campos de selecao possuem 0 ou 1.
            selecionado = (
                coluna(nome_coluna)
                .cast("double") == 1.0
            )

            item = F.when(
                selecionado,
                F.struct(

                    F.lit(categoria)
                    .alias("categoria"),

                    F.lit(tecnologia)
                    .alias("tecnologia"),

                    F.lit(rotulo_original)
                    .alias("tecnologia_original"),

                    F.lit(tipo_item)
                    .alias("tipo_item"),

                    F.lit(PERGUNTAS[ano][categoria])
                    .alias("pergunta_origem"),

                    F.lit(COMPARABILIDADE[ano][categoria])
                    .alias("comparabilidade")
                )
            )

            itens.append(item)

        # Cria um array com os itens marcados como 1
        # e remove os elementos nulos.

        array_itens = F.filter(
            F.array(*itens),
            lambda x: x.isNotNull()
        )

        categoria_df = (
            df
            .select(

                F.trim(
                    coluna(id_col).cast("string")
                ).alias("id_respondente"),

                F.lit(ano)
                .cast("int")
                .alias("ano_pesquisa"),

                F.explode(array_itens)
                .alias("item")
            )
            .select(
                "id_respondente",
                "ano_pesquisa",
                F.col("item.categoria").alias("categoria"),
                F.col("item.tecnologia").alias("tecnologia"),
                F.col("item.tecnologia_original").alias(
                    "tecnologia_original"
                ),
                F.col("item.tipo_item").alias("tipo_item"),
                F.col("item.pergunta_origem").alias(
                    "pergunta_origem"
                ),
                F.col("item.comparabilidade").alias(
                    "comparabilidade"
                )
            )
            .filter(
                F.col("id_respondente").isNotNull()
            )
        )

        dataframes_categorias.append(
            categoria_df
        )

    return reduce(
        lambda a, b: a.unionByName(b),
        dataframes_categorias
    )


# ============================================================
# 8. LEITURA DA BRONZE
# ============================================================

print("Lendo State of Data 2023...")
bronze_2023 = ler_csv(PATH_2023)

print("Lendo State of Data 2024...")
bronze_2024 = ler_csv(PATH_2024)

print("Lendo State of Data 2025...")
bronze_2025 = ler_csv(PATH_2025)


# ============================================================
# 9. TRANSFORMACAO DE CADA ANO
# ============================================================

tech_2023 = transformar_tecnologias(
    bronze_2023,
    2023
)

tech_2024 = transformar_tecnologias(
    bronze_2024,
    2024
)

tech_2025 = transformar_tecnologias(
    bronze_2025,
    2025
)


# ============================================================
# 10. UNION DOS TRES ANOS
# ============================================================

silver_tecnologias = (
    tech_2023
    .unionByName(tech_2024)
    .unionByName(tech_2025)

    # Remove repeticoes causadas pelos respondentes
    # duplicados encontrados nas bases de 2024/2025.
    .dropDuplicates([
        "ano_pesquisa",
        "id_respondente",
        "categoria",
        "tecnologia"
    ])
)


# Cache apenas para as validacoes seguintes.
silver_tecnologias.cache()


# ============================================================
# 11. VALIDACOES
# ============================================================

print("============================================")
print("VALIDACAO SILVER TECNOLOGIAS")
print("============================================")

total = silver_tecnologias.count()

print(f"TOTAL DE SELECOES: {total}")


print("Quantidade por ano:")

(
    silver_tecnologias
    .groupBy("ano_pesquisa")
    .count()
    .orderBy("ano_pesquisa")
    .show()
)


print("Quantidade por ano e categoria:")

(
    silver_tecnologias
    .groupBy(
        "ano_pesquisa",
        "categoria"
    )
    .count()
    .orderBy(
        "ano_pesquisa",
        "categoria"
    )
    .show(50, truncate=False)
)


print("Exemplo de tecnologias:")

(
    silver_tecnologias
    .select(
        "ano_pesquisa",
        "categoria",
        "tecnologia",
        "tipo_item"
    )
    .orderBy(
        "ano_pesquisa",
        "categoria",
        "tecnologia"
    )
    .show(50, truncate=False)
)


# ============================================================
# 12. GRAVACAO DA SILVER EM PARQUET
# ============================================================

(
    silver_tecnologias
    .repartition(
        3,
        "ano_pesquisa"
    )
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("ano_pesquisa")
    .save(SILVER_PATH)
)


print(
    f"Silver tecnologias gravada em: "
    f"{SILVER_PATH}"
)


# ============================================================
# 13. FINALIZACAO
# ============================================================

silver_tecnologias.unpersist()

job.commit()