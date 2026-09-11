# Tech Challenge — Fase 3
## State of Data Brasil 2023–2025

Este projeto foi desenvolvido para o Tech Challenge da Fase 3, com foco em Big Data Analytics na AWS.

A proposta foi reunir as pesquisas State of Data Brasil de 2023, 2024 e 2025 em um mesmo pipeline, tratar as diferenças entre os questionários e preparar uma camada analítica para explorar perfil profissional, remuneração, tecnologias e Inteligência Artificial.

## O que foi feito

O fluxo foi dividido em três camadas:

- **Bronze:** armazenamento dos CSVs originais no Amazon S3, sem alteração;
- **Silver:** harmonização dos campos entre os anos, limpeza, deduplicação e gravação em Parquet;
- **Gold:** criação de indicadores agregados para consulta no Athena e uso no Power BI.

As análises finais cobrem:

- senioridade;
- gênero;
- região;
- modelo de trabalho;
- remuneração;
- tecnologias;
- Inteligência Artificial.

## Arquitetura

```text
State of Data / Kaggle
        |
        v
Amazon S3 - Bronze (CSV bruto)
        |
        v
AWS Glue Jobs + PySpark
        |
        v
Amazon S3 - Silver (Parquet)
  - respondentes
  - tecnologias
  - ia
        |
        +--> Glue Crawlers / Data Catalog
        |
        v
AWS Glue Job - Gold Analytics
        |
        v
Amazon S3 - Gold (Parquet)
  - perfil_mercado
  - remuneracao
  - tecnologias
  - ia
        |
        +--> Glue Crawlers / Data Catalog
        |
        v
Amazon Athena
        |
        v
Power BI / apresentação final
```

O ambiente foi montado no AWS Academy Lab, na região `us-east-1`.

O arquivo editável da arquitetura está em:

```text
arquitetura/Arquitetura_AWS_TechChallenge_Fase3.drawio
```

## Organização do Data Lake

```text
bronze/
  state_of_data/
    ano=2023/
    ano=2024/
    ano=2025/

silver/
  respondentes/
  tecnologias/
  ia/

gold/
  perfil_mercado/
  remuneracao/
  tecnologias/
  ia/

athena-results/
```

As camadas Silver e Gold foram gravadas em Parquet e particionadas por `ano_pesquisa`.

## Processamento

### Silver de respondentes

O primeiro Glue Job padroniza os campos de perfil e mercado. Como os nomes e posições das perguntas mudam entre os anos, o mapeamento foi feito de forma explícita antes da união dos dados.

Script:

```text
scripts/tc-fase3-silver-respondentes.py
```

### Silver de tecnologias

As tecnologias aparecem nos arquivos originais em várias colunas binárias (`0/1`). O processamento transforma esse formato em uma tabela longa, com uma linha por tecnologia selecionada.

As categorias tratadas são linguagem, banco de dados, cloud e BI.

Script:

```text
scripts/tc-fase3-silver-tecnologias.py
```

### Silver de IA

A terceira Silver reúne as perguntas relacionadas a IA e LLMs em dimensões padronizadas, como prioridade na empresa, formas de uso, produtividade, barreiras e resultados com LLMs.

Script:

```text
scripts/tc-fase3-silver-ia.py
```

### Gold Analytics

A camada Gold consolida os indicadores utilizados nas análises finais:

- `gold_perfil_mercado`
- `gold_remuneracao`
- `gold_tecnologias`
- `gold_ia`

Script:

```text
scripts/tc-fase3-gold-analytics.py
```

## Catálogo e consultas

As tabelas Silver e Gold foram catalogadas com AWS Glue Crawlers no database:

```text
tc_fase3
```

O Amazon Athena foi usado para duas finalidades:

1. validar os resultados das transformações;
2. gerar as consultas que alimentaram os CSVs usados no Power BI.

As consultas consolidadas estão em:

```text
sql/consultas_athena.sql
```

## Validações principais

Depois da deduplicação, a Silver de respondentes ficou com:

| Ano | Respondentes |
|---:|---:|
| 2023 | 5.293 |
| 2024 | 5.215 |
| 2025 | 3.494 |
| **Total** | **14.002** |

Volumes finais das tabelas Gold:

| Tabela | Linhas |
|---|---:|
| `gold_perfil_mercado` | 186 |
| `gold_remuneracao` | 964 |
| `gold_tecnologias` | 207 |
| `gold_ia` | 86 |

## Alguns resultados encontrados

- Em 2025, Sênior + Especialista/Staff+ representam **48,28%** dos respondentes com senioridade informada.
- A participação feminina passa de **24,43% em 2023 para 21,95% em 2025**.
- O Sudeste representa **64,41%** dos respondentes em 2025.
- O trabalho 100% remoto passa de **46,31% em 2023 para 39,70% em 2025**.
- A soma de prioridade principal e alta prioridade para IA passa de **36,16% em 2023 para 60,58% em 2025**.
- Em 2025, na pergunta sobre resultados com LLMs, **38,39%** relatam pilotos com pouco impacto e **26,47%** produção com impacto.

## Cuidados na comparação entre os anos

Algumas mudanças no questionário exigem cautela na leitura dos resultados:

- as amostras têm tamanhos diferentes;
- a categoria `Especialista/Staff+` aparece somente em 2025;
- em 2025, a pergunta de linguagens mede preferência e não é estritamente igual às perguntas dos anos anteriores;
- a pergunta de Cloud de 2023 possui diferença de formulação;
- a pergunta sobre resultados com LLMs existe somente em 2025.

Os resultados descrevem os respondentes da pesquisa State of Data Brasil. Eles não devem ser tratados como uma estimativa censitária de todo o mercado brasileiro.

## Estrutura da entrega

```text
TechChallenge_Fase3/
├── scripts/
│   ├── tc-fase3-silver-respondentes.py
│   ├── tc-fase3-silver-tecnologias.py
│   ├── tc-fase3-silver-ia.py
│   └── tc-fase3-gold-analytics.py
├── sql/
│   └── consultas_athena.sql
├── notebook/
│   └── TechChallenge_Fase3.ipynb
├── arquitetura/
│   └── Arquitetura_AWS_TechChallenge_Fase3.drawio
├── powerbi/
│   ├── Tech3.pbix
│   └── dados/
├── apresentacao/
│   └── TechChallenge_Fase3.pptx
└── README.md
```

## Tecnologias utilizadas

- Amazon S3
- AWS Glue Jobs
- PySpark
- AWS Glue Crawlers
- AWS Glue Data Catalog
- Amazon Athena
- Power BI
- Draw.io

## Observação sobre os scripts

Os quatro arquivos da pasta `scripts/` foram preservados com a mesma lógica executada no AWS Glue. O notebook serve como documentação consolidada do projeto e reúne as principais decisões de tratamento, validações e consultas.
