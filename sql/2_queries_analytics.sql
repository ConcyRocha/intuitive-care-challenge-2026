-- ============================================================================
-- ARQUIVO: sql/2_queries_analytics.sql
-- OBJETIVO: Respostas Analíticas do Teste 3.4
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: Quais as 5 operadoras com maior crescimento percentual de despesas?
-- ----------------------------------------------------------------------------
-- Desafio: Operadoras podem não ter dados em todos os trimestres.
-- Solução: Utilizamos INNER JOIN entre o primeiro e o último trimestre disponível.
-- Isso elimina automaticamente quem não tem dados nas duas pontas (pois não dá 
-- para calcular crescimento sem ponto de partida).

WITH total_por_trimestre AS (
    SELECT 
        d.cnpj,
        o.razao_social,
        d.data_referencia,
        SUM(d.valor_despesa) as total_trimestre
    FROM fact_despesas d
    JOIN dim_operadoras o ON d.cnpj = o.cnpj
    GROUP BY 1, 2, 3
),
primeiro_tri AS (
    SELECT * FROM total_por_trimestre 
    WHERE data_referencia = (SELECT MIN(data_referencia) FROM fact_despesas)
    AND total_trimestre > 0 
),
ultimo_tri AS (
    SELECT * FROM total_por_trimestre 
    WHERE data_referencia = (SELECT MAX(data_referencia) FROM fact_despesas)
)
SELECT 
    u.razao_social,
    p.total_trimestre as valor_inicial,
    u.total_trimestre as valor_final,
    ROUND(((u.total_trimestre - p.total_trimestre) / p.total_trimestre) * 100, 2) as crescimento_pct
FROM ultimo_tri u
JOIN primeiro_tri p ON u.cnpj = p.cnpj -- Garante que existe em ambos
ORDER BY crescimento_pct DESC
LIMIT 5;

-- ----------------------------------------------------------------------------
-- QUERY 2: Top 5 estados com maiores despesas + Média por operadora
-- ----------------------------------------------------------------------------
-- Desafio Adicional: Calcular Total E Média na mesma query.
-- Solução: Agrupamento simples por UF resolve ambos.

SELECT 
    o.uf,
    SUM(d.valor_despesa) as total_despesas_estado,
    AVG(d.valor_despesa) as media_por_lancamento,
    COUNT(DISTINCT o.cnpj) as qtd_operadoras
FROM fact_despesas d
JOIN dim_operadoras o ON d.cnpj = o.cnpj
WHERE o.uf != 'Não Informado' -- Expurga inconsistências do cadastro
GROUP BY o.uf
ORDER BY total_despesas_estado DESC
LIMIT 5;

-- ----------------------------------------------------------------------------
-- QUERY 3: Operadoras com despesas acima da média em >= 2 trimestres
-- ----------------------------------------------------------------------------
-- Trade-off: Usei CTEs (Common Table Expressions) ao invés de Subqueries aninhadas.
-- Justificativa: Legibilidade. É muito mais fácil entender o passo a passo 
-- (calcula média -> compara -> conta) do que ler um bloco único de código.

WITH media_geral_trimestre AS (
    -- 1. Calcula a média do mercado para cada trimestre
    SELECT data_referencia, AVG(valor_despesa) as media_mercado
    FROM fact_despesas
    GROUP BY data_referencia
),
despesas_operadora AS (
    -- 2. Calcula a despesa de cada operadora por trimestre
    SELECT cnpj, data_referencia, SUM(valor_despesa) as total_op
    FROM fact_despesas
    GROUP BY 1, 2
),
comparativo AS (
    -- 3. Cruza os dados e cria uma flag (1 ou 0) se ficou acima da média
    SELECT 
        d.cnpj,
        CASE WHEN d.total_op > m.media_mercado THEN 1 ELSE 0 END as acima_da_media
    FROM despesas_operadora d
    JOIN media_geral_trimestre m ON d.data_referencia = m.data_referencia
)
-- 4. Agrupa e filtra quem teve flag 1 em pelo menos 2 trimestres
SELECT 
    c.cnpj, 
    o.razao_social,
    SUM(c.acima_da_media) as qtd_trimestres_acima
FROM comparativo c
JOIN dim_operadoras o ON c.cnpj = o.cnpj
GROUP BY 1, 2
HAVING SUM(c.acima_da_media) >= 2;