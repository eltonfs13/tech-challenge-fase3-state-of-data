import sys
import ast

from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


# ============================================================
# 1. INICIALIZACAO
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. CAMINHOS
# ============================================================

BUCKET = "fiap-techchallenge-fase3-221507154727"

PATHS = {
    2023: f"s3://{BUCKET}/bronze/state_of_data/ano=2023/",
    2024: f"s3://{BUCKET}/bronze/state_of_data/ano=2024/",
    2025: f"s3://{BUCKET}/bronze/state_of_data/ano=2025/"
}

SILVER_PATH = f"s3://{BUCKET}/silver/ia/"


# ============================================================
# 3. CONFIGURACAO DOS ANOS
# ============================================================

CONFIG = {

    2023: {
        "id": "('P0', 'id')",

        "prioridade":
            "('P3_e ', 'AI Generativa é uma prioridade em sua empresa?')",

        "resultado": None,

        # A mesma pergunta aparece em dois fluxos do questionario.
        # Os conjuntos sao mutuamente exclusivos por respondente.
        "tipo_uso_blocos": [
            "P3_f_",
            "P4_l_"
        ],

        "produtividade": "P4_m_",
        "barreiras": "P3_g_"
    },

    2024: {
        "id": "0.a_token",

        "prioridade":
            "3.e_ai_generativa_e_llm_é_uma_prioridade?",

        "resultado": None,

        "tipo_uso_blocos": [
            "3.f.",
            "4.l."
        ],

        "produtividade": "4.m.",
        "barreiras": "3.g."
    },

    2025: {
        "id": "0.a_token",

        "prioridade":
            "3.e_ai_generativa_e_llm_é_uma_prioridade?",

        "resultado":
            "3.g_empresa_está_conseguindo_ter_bons_resultados_com_llms",

        "tipo_uso_blocos": [
            "3.f.",
            "4.i."
        ],

        "produtividade": "4.j.",
        "barreiras": "3.h."
    }
}


# ============================================================
# 4. CATEGORIAS PADRONIZADAS
# ============================================================

TIPO_USO = {
    1: "uso_descentralizado",
    2: "direcionamento_centralizado",
    3: "copilots_desenvolvedores",
    4: "produtos_externos",
    5: "produtos_internos",
    6: "principal_frente_negocio",
    7: "nao_prioridade",
    8: "nao_sabe"
}


PRODUTIVIDADE = {
    1: "nao_usa",
    2: "usa_solucao_gratuita",
    3: "usa_e_paga",
    4: "empresa_paga",
    5: "usa_copilot"
}


BARREIRAS = {
    1: "falta_compreensao_casos_uso",
    2: "falta_confiabilidade_alucinacoes",
    3: "incerteza_regulatoria",
    4: "seguranca_privacidade_dados",
    5: "roi_nao_comprovado",
    6: "dados_nao_prontos",
    7: "falta_expertise_recursos",
    8: "alta_direcao_nao_ve_valor",
    9: "propriedade_intelectual"
}


# ============================================================
# 5. FUNCOES AUXILIARES
# ============================================================

def ler_csv(caminho):

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

    nome_seguro = nome.replace("`", "``")

    return F.col(
        f"`{nome_seguro}`"
    )


def localizar_coluna(df, ano, codigo):
    """
    Localiza colunas de multisselecao.

    2023:
    ('P3_f_1 ', 'texto...')

    2024/2025:
    3.f.1 texto...
    """

    if ano == 2023:

        prefixo = f"('{codigo} "

        encontradas = [
            c for c in df.columns
            if c.startswith(prefixo)
        ]

    else:

        prefixo = f"{codigo} "

        encontradas = [
            c for c in df.columns
            if c.startswith(prefixo)
        ]

    if len(encontradas) != 1:

        raise ValueError(
            f"Coluna nao encontrada ou ambigua: "
            f"ano={ano}, codigo={codigo}, "
            f"encontradas={encontradas}"
        )

    return encontradas[0]


def extrair_rotulo(nome_coluna, ano):

    if ano == 2023:

        try:

            valor = ast.literal_eval(
                nome_coluna
            )

            return str(
                valor[1]
            ).strip()

        except Exception:

            return nome_coluna

    if " " in nome_coluna:

        return nome_coluna.split(
            " ",
            1
        )[1].strip()

    return nome_coluna


# ============================================================
# 6. NORMALIZACAO DAS RESPOSTAS CATEGORICAS
# ============================================================

def normalizar_prioridade(nome_coluna):

    texto = F.lower(
        F.trim(
            coluna(nome_coluna)
        )
    )

    return (
        F.when(
            texto.contains("principal prioridade como empresa"),
            F.lit("prioridade_principal")
        )

        .when(
            texto.contains("principais prioridades"),
            F.lit("alta_prioridade_2_4_anos")
        )

        .when(
            texto.contains("uma das várias iniciativas"),
            F.lit("iniciativa_secundaria")
        )

        .when(
            texto.contains("uma das varias iniciativas"),
            F.lit("iniciativa_secundaria")
        )

        .when(
            texto.contains("não é uma iniciativa"),
            F.lit("nao_prioridade")
        )

        .when(
            texto.contains("nao é uma iniciativa"),
            F.lit("nao_prioridade")
        )

        .when(
            texto.contains("não sei"),
            F.lit("nao_sabe")
        )

        .when(
            texto.contains("nao sei"),
            F.lit("nao_sabe")
        )

        .otherwise(
            F.trim(
                coluna(nome_coluna)
            )
        )
    )


def normalizar_resultado_llm(nome_coluna):

    texto = F.lower(
        F.trim(
            coluna(nome_coluna)
        )
    )

    return (
        F.when(
            texto.contains("em produção"),
            F.lit("producao_com_impacto")
        )

        .when(
            texto.contains("em producao"),
            F.lit("producao_com_impacto")
        )

        .when(
            texto.contains("projetos \"piloto\""),
            F.lit("pilotos_pouco_impacto")
        )

        .when(
            texto.contains("investigação e planejamento"),
            F.lit("investigacao_planejamento")
        )

        .when(
            texto.contains("investigacao e planejamento"),
            F.lit("investigacao_planejamento")
        )

        .when(
            texto.contains("ainda não começamos"),
            F.lit("nao_iniciou")
        )

        .when(
            texto.contains("ainda nao comecamos"),
            F.lit("nao_iniciou")
        )

        .when(
            texto.contains("não sei"),
            F.lit("nao_sabe")
        )

        .when(
            texto.contains("nao sei"),
            F.lit("nao_sabe")
        )

        .otherwise(
            F.trim(
                coluna(nome_coluna)
            )
        )
    )


# ============================================================
# 7. RESPOSTAS CATEGORICAS
# ============================================================

def criar_prioridade(df, ano):

    config = CONFIG[ano]

    campo = config["prioridade"]

    return (
        df
        .filter(
            coluna(config["id"]).isNotNull()
            & coluna(campo).isNotNull()
        )
        .select(

            F.trim(
                coluna(
                    config["id"]
                ).cast("string")
            ).alias(
                "id_respondente"
            ),

            F.lit(ano)
            .cast("int")
            .alias("ano_pesquisa"),

            F.lit(
                "prioridade_empresa"
            ).alias("dimensao"),

            normalizar_prioridade(
                campo
            ).alias("resposta"),

            F.trim(
                coluna(campo)
            ).alias(
                "resposta_original"
            ),

            F.lit(
                "categorica"
            ).alias("tipo_resposta"),

            F.lit(
                "comparavel_2023_2025"
            ).alias("comparabilidade"),

            F.lit(
                "P3_e / 3.e"
            ).alias("bloco_origem")
        )
    )


def criar_resultado_2025(df):

    campo = CONFIG[2025]["resultado"]

    return (
        df
        .filter(
            coluna(
                CONFIG[2025]["id"]
            ).isNotNull()
            & coluna(campo).isNotNull()
        )
        .select(

            F.trim(
                coluna(
                    CONFIG[2025]["id"]
                ).cast("string")
            ).alias(
                "id_respondente"
            ),

            F.lit(2025)
            .cast("int")
            .alias("ano_pesquisa"),

            F.lit(
                "resultado_llm_empresa"
            ).alias("dimensao"),

            normalizar_resultado_llm(
                campo
            ).alias("resposta"),

            F.trim(
                coluna(campo)
            ).alias(
                "resposta_original"
            ),

            F.lit(
                "categorica"
            ).alias("tipo_resposta"),

            F.lit(
                "somente_2025"
            ).alias("comparabilidade"),

            F.lit(
                "3.g"
            ).alias("bloco_origem")
        )
    )


# ============================================================
# 8. RESPOSTAS DE MULTISSELECAO
# ============================================================

def criar_multisselecao(
    df,
    ano,
    dimensao,
    prefixo,
    mapa_respostas,
    comparabilidade
):

    config = CONFIG[ano]

    partes = []

    for numero, resposta in mapa_respostas.items():

        codigo = (
            f"{prefixo}{numero}"
        )

        nome_coluna = localizar_coluna(
            df,
            ano,
            codigo
        )

        rotulo_original = extrair_rotulo(
            nome_coluna,
            ano
        )

        parte = (
            df

            .filter(
                coluna(
                    config["id"]
                ).isNotNull()

                & (
                    coluna(
                        nome_coluna
                    ).cast("double")
                    == 1.0
                )
            )

            .select(

                F.trim(
                    coluna(
                        config["id"]
                    ).cast("string")
                ).alias(
                    "id_respondente"
                ),

                F.lit(ano)
                .cast("int")
                .alias("ano_pesquisa"),

                F.lit(
                    dimensao
                ).alias("dimensao"),

                F.lit(
                    resposta
                ).alias("resposta"),

                F.lit(
                    rotulo_original
                ).alias(
                    "resposta_original"
                ),

                F.lit(
                    "multisselecao"
                ).alias(
                    "tipo_resposta"
                ),

                F.lit(
                    comparabilidade
                ).alias(
                    "comparabilidade"
                ),

                F.lit(
                    prefixo
                ).alias(
                    "bloco_origem"
                )
            )
        )

        partes.append(
            parte
        )

    return reduce(
        lambda a, b:
            a.unionByName(b),
        partes
    )


# ============================================================
# 9. PROCESSAMENTO DE UM ANO
# ============================================================

def transformar_ano(df, ano):

    config = CONFIG[ano]

    partes = []

    # Prioridade da IA na empresa
    partes.append(
        criar_prioridade(
            df,
            ano
        )
    )

    # Tipos de uso de IA:
    # existem dois blocos equivalentes no questionario,
    # destinados a fluxos distintos de respondentes.

    for prefixo in config[
        "tipo_uso_blocos"
    ]:

        partes.append(
            criar_multisselecao(
                df=df,
                ano=ano,
                dimensao="tipo_uso_empresa",
                prefixo=prefixo,
                mapa_respostas=TIPO_USO,
                comparabilidade="comparavel_2023_2025"
            )
        )

    # Uso pessoal de IA para produtividade
    partes.append(
        criar_multisselecao(
            df=df,
            ano=ano,
            dimensao="produtividade_pessoal",
            prefixo=config[
                "produtividade"
            ],
            mapa_respostas=PRODUTIVIDADE,
            comparabilidade="comparavel_2023_2025"
        )
    )

    # Barreiras de adocao
    partes.append(
        criar_multisselecao(
            df=df,
            ano=ano,
            dimensao="barreira_adocao",
            prefixo=config[
                "barreiras"
            ],
            mapa_respostas=BARREIRAS,
            comparabilidade="comparavel_2023_2025"
        )
    )

    # Pergunta nova de 2025:
    # empresa esta conseguindo gerar resultados com LLM?
    if ano == 2025:

        partes.append(
            criar_resultado_2025(
                df
            )
        )

    return reduce(
        lambda a, b:
            a.unionByName(b),
        partes
    )


# ============================================================
# 10. LEITURA DA BRONZE
# ============================================================

print("Lendo pesquisa 2023...")

bronze_2023 = ler_csv(
    PATHS[2023]
)

print("Lendo pesquisa 2024...")

bronze_2024 = ler_csv(
    PATHS[2024]
)

print("Lendo pesquisa 2025...")

bronze_2025 = ler_csv(
    PATHS[2025]
)


# ============================================================
# 11. TRANSFORMACAO
# ============================================================

ia_2023 = transformar_ano(
    bronze_2023,
    2023
)

ia_2024 = transformar_ano(
    bronze_2024,
    2024
)

ia_2025 = transformar_ano(
    bronze_2025,
    2025
)


silver_ia = (
    ia_2023
    .unionByName(ia_2024)
    .unionByName(ia_2025)

    # Remove respostas repetidas decorrentes
    # dos poucos IDs duplicados na fonte.
    .dropDuplicates([
        "ano_pesquisa",
        "id_respondente",
        "dimensao",
        "resposta"
    ])
)


silver_ia.cache()


# ============================================================
# 12. VALIDACOES
# ============================================================

print("==========================================")
print("VALIDACAO SILVER IA")
print("==========================================")


print(
    f"TOTAL DE RESPOSTAS IA: "
    f"{silver_ia.count()}"
)


print(
    "Quantidade por ano e dimensao:"
)

(
    silver_ia
    .groupBy(
        "ano_pesquisa",
        "dimensao"
    )
    .count()
    .orderBy(
        "ano_pesquisa",
        "dimensao"
    )
    .show(
        50,
        truncate=False
    )
)


print(
    "Distribuicao das respostas:"
)

(
    silver_ia
    .groupBy(
        "ano_pesquisa",
        "dimensao",
        "resposta"
    )
    .count()
    .orderBy(
        "ano_pesquisa",
        "dimensao",
        F.desc("count")
    )
    .show(
        100,
        truncate=False
    )
)


# ============================================================
# 13. GRAVACAO EM PARQUET
# ============================================================

(
    silver_ia

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
        SILVER_PATH
    )
)


print(
    f"Silver IA gravada em: "
    f"{SILVER_PATH}"
)


silver_ia.unpersist()

job.commit()