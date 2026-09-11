import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


# ============================================================
# 1. INICIALIZACAO DO AWS GLUE / SPARK
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

SILVER_PATH = f"s3://{BUCKET}/silver/respondentes/"


# ============================================================
# 3. MAPEAMENTO DOS SCHEMAS
# Os nomes das perguntas mudam entre os anos.
# Aqui definimos a equivalencia entre elas.
# ============================================================

MAP_2023 = {
    "id_respondente": "('P0', 'id')",
    "idade": "('P1_a ', 'Idade')",
    "faixa_idade": "('P1_a_1 ', 'Faixa idade')",
    "genero": "('P1_b ', 'Genero')",
    "raca_etnia": "('P1_c ', 'Cor/raca/etnia')",
    "pcd": "('P1_d ', 'PCD')",
    "vive_brasil": "('P1_g ', 'vive_no_brasil')",
    "uf": "('P1_i_1 ', 'uf onde mora')",
    "regiao": "('P1_i_2 ', 'Regiao onde mora')",
    "nivel_ensino": "('P1_l ', 'Nivel de Ensino')",
    "area_formacao": "('P1_m ', 'Área de Formação')",
    "situacao_trabalho": "('P2_a ', 'Qual sua situação atual de trabalho?')",
    "setor": "('P2_b ', 'Setor')",
    "numero_funcionarios": "('P2_c ', 'Numero de Funcionarios')",
    "atua_como_gestor": "('P2_d ', 'Gestor?')",
    "cargo_atual": "('P2_f ', 'Cargo Atual')",
    "senioridade": "('P2_g ', 'Nivel')",
    "faixa_salarial": "('P2_h ', 'Faixa salarial')",
    "tempo_experiencia_dados": "('P2_i ', 'Quanto tempo de experiência na área de dados você tem?')",
    "tempo_experiencia_ti": "('P2_j ', 'Quanto tempo de experiência na área de TI/Engenharia de Software você teve antes de começar a trabalhar na área de dados?')",
    "satisfeito_emprego": "('P2_k ', 'Você está satisfeito na sua empresa atual?')",
    "modelo_trabalho_atual": "('P2_r ', 'Atualmente qual a sua forma de trabalho?')",
    "modelo_trabalho_ideal": "('P2_s ', 'Qual a forma de trabalho ideal para você?')"
}


MAP_2024 = {
    "id_respondente": "0.a_token",
    "idade": "1.a_idade",
    "faixa_idade": "1.a.1_faixa_idade",
    "genero": "1.b_genero",
    "raca_etnia": "1.c_cor/raca/etnia",
    "pcd": "1.d_pcd",
    "vive_brasil": "1.g_vive_no_brasil",
    "uf": "1.i.1_uf_onde_mora",
    "regiao": "1.i.2_regiao_onde_mora",
    "nivel_ensino": "1.l_nivel_de_ensino",
    "area_formacao": "1.m_área_de_formação",
    "situacao_trabalho": "2.a_situação_de_trabalho",
    "setor": "2.b_setor",
    "numero_funcionarios": "2.c_numero_de_funcionarios",
    "atua_como_gestor": "2.d_atua_como_gestor",
    "cargo_atual": "2.f_cargo_atual",
    "senioridade": "2.g_nivel",
    "faixa_salarial": "2.h_faixa_salarial",
    "tempo_experiencia_dados": "2.i_tempo_de_experiencia_em_dados",
    "tempo_experiencia_ti": "2.j_tempo_de_experiencia_em_ti",
    "satisfeito_emprego": "2.k_satisfeito_atualmente",
    "modelo_trabalho_atual": "2.r_modelo_de_trabalho_atual",
    "modelo_trabalho_ideal": "2.s_modelo_de_trabalho_ideal"
}


MAP_2025 = {
    "id_respondente": "0.a_token",
    "idade": "1.a_idade",
    "faixa_idade": "1.a.1_faixa_idade",
    "genero": "1.b_genero",
    "raca_etnia": "1.c_cor/raca/etnia",
    "pcd": "1.d_pcd",
    "vive_brasil": "1.g_vive_no_brasil",
    "uf": "1.i.1_uf_onde_mora",
    "regiao": "1.i.2_regiao_onde_mora",
    "nivel_ensino": "1.l_nivel_de_ensino",
    "area_formacao": "1.m_área_de_formação",
    "situacao_trabalho": "2.a_situação_de_trabalho",
    "setor": "2.b_setor",
    "numero_funcionarios": "2.c_numero_de_funcionarios",
    "atua_como_gestor": "2.d_atua_como_gestor",
    "cargo_atual": "2.f_cargo_atual",
    "senioridade": "2.g_nivel",
    "faixa_salarial": "2.h_faixa_salarial",
    "tempo_experiencia_dados": "2.i_tempo_de_experiencia_em_dados",
    "tempo_experiencia_ti": "2.j_tempo_de_experiencia_em_ti",
    "satisfeito_emprego": "2.k_satisfeito_atualmente",
    "modelo_trabalho_atual": "2.q_modelo_de_trabalho_atual",
    "modelo_trabalho_ideal": "2.r_modelo_de_trabalho_ideal"
}


# ============================================================
# 4. FUNCOES AUXILIARES
# ============================================================

def ler_csv(caminho):
    """
    Le o CSV da camada Bronze.
    Todos os campos sao inicialmente tratados como string
    para evitar inferencias diferentes entre os anos.
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
    barras, parenteses e outros caracteres especiais.
    """
    nome_seguro = nome.replace("`", "``")
    return F.col(f"`{nome_seguro}`")


def texto_limpo(nome):
    """
    Remove espacos nas extremidades e converte
    strings vazias em NULL.
    """
    valor = F.trim(coluna(nome).cast("string"))

    return (
        F.when(valor == "", F.lit(None))
        .otherwise(valor)
    )


def harmonizar(df, ano, mapa):
    """
    Seleciona as variaveis equivalentes de cada pesquisa
    e aplica um schema padrao para a camada Silver.
    """

    resultado = df.select(

        texto_limpo(mapa["id_respondente"]).alias("id_respondente"),

        F.lit(ano).cast("int").alias("ano_pesquisa"),

        coluna(mapa["idade"]).cast("int").alias("idade"),

        texto_limpo(mapa["faixa_idade"]).alias("faixa_idade"),
        texto_limpo(mapa["genero"]).alias("genero"),
        texto_limpo(mapa["raca_etnia"]).alias("raca_etnia"),
        texto_limpo(mapa["pcd"]).alias("pcd"),
        texto_limpo(mapa["vive_brasil"]).alias("vive_brasil"),
        texto_limpo(mapa["uf"]).alias("uf"),
        texto_limpo(mapa["regiao"]).alias("regiao"),
        texto_limpo(mapa["nivel_ensino"]).alias("nivel_ensino"),
        texto_limpo(mapa["area_formacao"]).alias("area_formacao"),
        texto_limpo(mapa["situacao_trabalho"]).alias("situacao_trabalho"),
        texto_limpo(mapa["setor"]).alias("setor"),
        texto_limpo(mapa["numero_funcionarios"]).alias("numero_funcionarios"),
        texto_limpo(mapa["atua_como_gestor"]).alias("atua_como_gestor"),
        texto_limpo(mapa["cargo_atual"]).alias("cargo_atual"),
        texto_limpo(mapa["senioridade"]).alias("senioridade"),

        # Mantemos o valor original para preservar rastreabilidade
        texto_limpo(mapa["faixa_salarial"]).alias("faixa_salarial_original"),

        texto_limpo(mapa["tempo_experiencia_dados"]).alias(
            "tempo_experiencia_dados"
        ),

        texto_limpo(mapa["tempo_experiencia_ti"]).alias(
            "tempo_experiencia_ti"
        ),

        texto_limpo(mapa["satisfeito_emprego"]).alias(
            "satisfeito_emprego"
        ),

        texto_limpo(mapa["modelo_trabalho_atual"]).alias(
            "modelo_trabalho_atual"
        ),

        texto_limpo(mapa["modelo_trabalho_ideal"]).alias(
            "modelo_trabalho_ideal"
        )
    )

    # Remove registros repetidos com o mesmo ID dentro do mesmo ano
    resultado = resultado.dropDuplicates(
        ["ano_pesquisa", "id_respondente"]
    )

    return resultado


# ============================================================
# 5. LEITURA DA CAMADA BRONZE
# ============================================================

print("Lendo State of Data 2023...")
bronze_2023 = ler_csv(PATH_2023)

print("Lendo State of Data 2024...")
bronze_2024 = ler_csv(PATH_2024)

print("Lendo State of Data 2025...")
bronze_2025 = ler_csv(PATH_2025)


# ============================================================
# 6. HARMONIZACAO
# ============================================================

silver_2023 = harmonizar(bronze_2023, 2023, MAP_2023)
silver_2024 = harmonizar(bronze_2024, 2024, MAP_2024)
silver_2025 = harmonizar(bronze_2025, 2025, MAP_2025)


# ============================================================
# 7. UNIAO DOS TRES ANOS
# ============================================================

silver_respondentes = (
    silver_2023
    .unionByName(silver_2024)
    .unionByName(silver_2025)
)


# ============================================================
# 8. VALIDACOES
# ============================================================

print("========================================")
print("VALIDACAO DA CAMADA SILVER")
print("========================================")

print(f"2023: {silver_2023.count()} registros")
print(f"2024: {silver_2024.count()} registros")
print(f"2025: {silver_2025.count()} registros")

total = silver_respondentes.count()

print(f"TOTAL SILVER: {total} registros")

print("Schema:")
silver_respondentes.printSchema()

print("Distribuicao por ano:")
silver_respondentes.groupBy("ano_pesquisa").count().orderBy(
    "ano_pesquisa"
).show()


# ============================================================
# 9. GRAVACAO DA CAMADA SILVER
# Formato Parquet + particionamento por ano
# ============================================================

(
    silver_respondentes
    .repartition(3, "ano_pesquisa")
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("ano_pesquisa")
    .save(SILVER_PATH)
)

print(f"Silver gravada com sucesso em: {SILVER_PATH}")


# ============================================================
# 10. FINALIZACAO DO JOB
# ============================================================

job.commit()