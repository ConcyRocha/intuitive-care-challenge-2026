-- ============================================================================
-- ARQUIVO: sql/1_schema_ddl.sql
-- OBJETIVO: Estrutura do Banco de Dados (PostgreSQL / MySQL Compatible)
-- ============================================================================

-- TRADE-OFF 1: NORMALIZAÇÃO (OPÇÃO ESCOLHIDA)
-- Escolhi separar os dados em Tabela Fato (Despesas) e Tabela Dimensão (Operadoras).
-- Justificativa: 
-- 1. Redução de Redundância: O endereço/UF da operadora se repete milhares de vezes 
--    nas despesas. Separar economiza armazenamento.
-- 2. Integridade: Se uma operadora muda de UF, atualizamos em apenas um lugar.
-- 3. Performance: Queries que somam valores (SUM) rodam mais rápido em tabelas 
--    fatos "estreitas" (menos colunas de texto).

CREATE TABLE dim_operadoras (
    cnpj VARCHAR(20) PRIMARY KEY,
    razao_social VARCHAR(255),
    registro_ans VARCHAR(20),
    modalidade VARCHAR(100),
    uf CHAR(2)
);

CREATE TABLE fact_despesas (
    id SERIAL PRIMARY KEY, 
    cnpj VARCHAR(20),
    data_referencia DATE,
    conta_contabil VARCHAR(50), -
    
    -- TRADE-OFF 2: TIPOS DE DADOS (DECIMAL vs FLOAT)
    -- Escolhido: DECIMAL(15, 2)
    -- Justificativa: Para dados financeiros, FLOAT gera erros de precisão 
    -- (arredondamento de ponto flutuante). DECIMAL garante a exatidão dos centavos.
    valor_despesa DECIMAL(15, 2),
    
    FOREIGN KEY (cnpj) REFERENCES dim_operadoras(cnpj)
);

-- Índices para acelerar as buscas das queries analíticas
CREATE INDEX idx_despesas_data ON fact_despesas(data_referencia);
CREATE INDEX idx_despesas_cnpj ON fact_despesas(cnpj);