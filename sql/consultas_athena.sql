-- ============================================================
-- Tech Challenge - Fase 3
-- Consultas utilizadas para validacao e analise no Amazon Athena
-- Database: tc_fase3
-- ============================================================


-- ============================================================
-- 1. VALIDACOES DA CAMADA SILVER
-- ============================================================

-- 1.1 Quantidade de respondentes por ano
SELECT
    ano_pesquisa,
    COUNT(*) AS quantidade_respondentes
FROM silver_respondentes
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;


-- 1.2 Distribuicao de senioridade na Silver
WITH base AS (
    SELECT
        ano_pesquisa,
        senioridade,
        COUNT(*) AS quantidade
    FROM silver_respondentes
    WHERE senioridade IS NOT NULL
    GROUP BY ano_pesquisa, senioridade
)
SELECT
    ano_pesquisa,
    senioridade,
    quantidade,
    ROUND(
        100.0 * quantidade /
        SUM(quantidade) OVER (PARTITION BY ano_pesquisa),
        2
    ) AS percentual
FROM base
ORDER BY ano_pesquisa, quantidade DESC;


-- 1.3 Quantidade de selecoes de tecnologia por ano e categoria
SELECT
    ano_pesquisa,
    categoria,
    COUNT(*) AS quantidade_selecoes
FROM silver_tecnologias
GROUP BY ano_pesquisa, categoria
ORDER BY ano_pesquisa, categoria;


-- 1.4 Ranking detalhado de tecnologias na Silver
SELECT
    ano_pesquisa,
    categoria,
    tecnologia,
    COUNT(DISTINCT id_respondente) AS qtd_profissionais
FROM silver_tecnologias
WHERE tipo_item = 'tecnologia'
GROUP BY ano_pesquisa, categoria, tecnologia
ORDER BY ano_pesquisa, categoria, qtd_profissionais DESC;


-- 1.5 Quantidade de respostas de IA por ano e dimensao
SELECT
    ano_pesquisa,
    dimensao,
    COUNT(*) AS quantidade
FROM silver_ia
GROUP BY ano_pesquisa, dimensao
ORDER BY ano_pesquisa, dimensao;


-- 1.6 Prioridade de IA calculada a partir da Silver
WITH base AS (
    SELECT
        ano_pesquisa,
        resposta,
        COUNT(DISTINCT id_respondente) AS quantidade
    FROM silver_ia
    WHERE dimensao = 'prioridade_empresa'
    GROUP BY ano_pesquisa, resposta
),
totais AS (
    SELECT
        ano_pesquisa,
        COUNT(DISTINCT id_respondente) AS total
    FROM silver_ia
    WHERE dimensao = 'prioridade_empresa'
    GROUP BY ano_pesquisa
)
SELECT
    b.ano_pesquisa,
    b.resposta,
    b.quantidade,
    t.total AS total_respondentes,
    ROUND(100.0 * b.quantidade / t.total, 2) AS percentual
FROM base b
JOIN totais t
    ON b.ano_pesquisa = t.ano_pesquisa
ORDER BY b.ano_pesquisa, percentual DESC;


-- 1.7 Resultado dos projetos com LLMs em 2025
WITH base AS (
    SELECT
        resposta,
        COUNT(DISTINCT id_respondente) AS quantidade
    FROM silver_ia
    WHERE ano_pesquisa = '2025'
      AND dimensao = 'resultado_llm_empresa'
    GROUP BY resposta
)
SELECT
    resposta,
    quantidade,
    ROUND(
        100.0 * quantidade / SUM(quantidade) OVER (),
        2
    ) AS percentual
FROM base
ORDER BY quantidade DESC;


-- ============================================================
-- 2. VALIDACOES DA CAMADA GOLD
-- ============================================================

-- 2.1 Quantidade de linhas da Gold de perfil por ano
SELECT
    ano_pesquisa,
    COUNT(*) AS linhas
FROM gold_perfil_mercado
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;


-- 2.2 Quantidade de linhas das quatro tabelas Gold
SELECT 'perfil_mercado' AS tabela, COUNT(*) AS linhas
FROM gold_perfil_mercado
UNION ALL
SELECT 'remuneracao', COUNT(*)
FROM gold_remuneracao
UNION ALL
SELECT 'tecnologias', COUNT(*)
FROM gold_tecnologias
UNION ALL
SELECT 'ia', COUNT(*)
FROM gold_ia;


-- ============================================================
-- 3. CONSULTAS ANALITICAS FINAIS
--    Estas consultas geraram os CSVs usados no Power BI.
-- ============================================================

-- 3.1 Senioridade
SELECT
    ano_pesquisa,
    categoria AS senioridade,
    quantidade,
    total_validos,
    percentual,
    ranking
FROM gold_perfil_mercado
WHERE dimensao = 'senioridade'
ORDER BY ano_pesquisa, ranking;


-- 3.2 Genero
SELECT
    ano_pesquisa,
    categoria AS genero,
    quantidade,
    total_validos,
    percentual
FROM gold_perfil_mercado
WHERE dimensao = 'genero'
ORDER BY ano_pesquisa, percentual DESC;


-- 3.3 Regiao
SELECT
    ano_pesquisa,
    categoria AS regiao,
    quantidade,
    percentual
FROM gold_perfil_mercado
WHERE dimensao = 'regiao'
ORDER BY ano_pesquisa, percentual DESC;


-- 3.4 Modelo de trabalho atual
SELECT
    ano_pesquisa,
    categoria AS modelo_trabalho,
    quantidade,
    percentual
FROM gold_perfil_mercado
WHERE dimensao = 'modelo_trabalho_atual'
ORDER BY ano_pesquisa, percentual DESC;


-- 3.5 Remuneracao por senioridade
SELECT
    ano_pesquisa,
    categoria_analise AS senioridade,
    faixa_salarial_original AS faixa_salarial,
    quantidade,
    percentual,
    faixa_salarial_ordem
FROM gold_remuneracao
WHERE dimensao_analise = 'senioridade'
  AND anomalia_faixa_salarial = false
ORDER BY ano_pesquisa, senioridade, faixa_salarial_ordem;


-- 3.6 Top 5 tecnologias por categoria e ano
SELECT
    ano_pesquisa,
    categoria,
    ranking,
    tecnologia,
    qtd_profissionais,
    percentual_adocao,
    comparabilidade
FROM gold_tecnologias
WHERE ranking <= 5
ORDER BY ano_pesquisa, categoria, ranking;


-- 3.7 Indicadores de IA usados na visualizacao final
SELECT
    ano_pesquisa,
    dimensao,
    resposta,
    qtd_respondentes,
    percentual,
    ranking,
    percentuais_somam_100
FROM gold_ia
WHERE dimensao IN (
    'prioridade_empresa',
    'resultado_llm_empresa'
)
ORDER BY ano_pesquisa, dimensao, ranking;
