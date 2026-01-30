import sqlite3
import pandas as pd
import os

DB_NAME = "teste_intu.db"
SQL_FILE = os.path.join("sql", "2_queries_analytics.sql")
CSV_ENRIQUECIDO = os.path.join("data", "processed", "dados_enriquecido.csv")
CSV_VALIDO = os.path.join("data", "processed", "despesas_validas.csv")


def find_column(df, candidates):
    """Encontra a primeira coluna que bate com a lista de candidatos."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def reconstruir_data(row):
    """
    Reconstrói uma data válida (YYYY-MM-DD) a partir de Ano e Trimestre.
    Assume o primeiro dia do trimestre.
    """
    ano = str(row['Ano'])
    tri = str(row['Trimestre'])
    
    if '1' in tri: month = '01'
    elif '2' in tri: month = '04'
    elif '3' in tri: month = '07'
    elif '4' in tri: month = '10'
    else: month = '01' # Fallback
    
    return f"{ano}-{month}-01"

def create_and_load_db():
    print(">>> Iniciando Teste de Banco de Dados (SQLite Lab)")
    
    conn = sqlite3.connect(DB_NAME)
    print(f"   -> Banco de dados criado: {DB_NAME}")

    print("   -> Carregando CSVs...")
    if not os.path.exists(CSV_ENRIQUECIDO) or not os.path.exists(CSV_VALIDO):
        print(f"ERRO CRÍTICO: Arquivos CSV não encontrados.")
        exit()

    df_full = pd.read_csv(CSV_ENRIQUECIDO, sep=';', encoding='utf-8')
    df_despesas = pd.read_csv(CSV_VALIDO, sep=';', encoding='utf-8')

    print(f"   -> Colunas no CSV: {list(df_despesas.columns)}")

    # --- LÓGICA DE CORREÇÃO DE DATA ---
    # Se existirem as colunas separadas, reconstruímos a data
    if 'Trimestre' in df_despesas.columns and 'Ano' in df_despesas.columns:
        print("   -> Reconstruindo coluna 'data_referencia' a partir de Ano/Trimestre...")
        df_despesas['data_referencia'] = df_despesas.apply(reconstruir_data, axis=1)
        col_data = 'data_referencia'
    else:
        # Tenta achar uma coluna de data normal
        col_data = find_column(df_despesas, ['Data', 'DATA', 'data', 'DATA_EVENTO'])
        if not col_data:
            raise KeyError("Não foi possível identificar a Data (nem coluna Data, nem Ano/Trimestre).")

    # Identifica colunas restantes
    col_valor = find_column(df_despesas, ['ValorDespesas', 'VALOR', 'valor'])
    col_cnpj = find_column(df_despesas, ['CNPJ', 'cnpj'])

    # Padronização para o SQL
    df_fact = df_despesas[[col_cnpj, col_data, col_valor]].copy()
    df_fact.columns = ['cnpj', 'data_referencia', 'valor_despesa']

    # Ajuste de Tipos
    df_fact['valor_despesa'] = pd.to_numeric(df_fact['valor_despesa'], errors='coerce').fillna(0)

    # Preparação da Dimensão Operadoras
    col_uf = find_column(df_full, ['UF', 'uf'])
    col_razao = find_column(df_full, ['RazaoSocial', 'RAZAO_SOCIAL'])
    
    # Se não achar modalidade, cria vazia (resiliência)
    col_mod = find_column(df_full, ['Modalidade', 'MODALIDADE'])
    if not col_mod:
        df_full['Modalidade'] = 'N/A'
        col_mod = 'Modalidade'

    df_dim = df_full[[col_cnpj, col_razao, col_uf, col_mod]].drop_duplicates(subset=[col_cnpj])
    df_dim.columns = ['cnpj', 'razao_social', 'uf', 'modalidade']

    # Carga no Banco
    print("   -> Inserindo dados na tabela 'dim_operadoras'...")
    df_dim.to_sql('dim_operadoras', conn, if_exists='replace', index=False)

    print("   -> Inserindo dados na tabela 'fact_despesas'...")
    df_fact.to_sql('fact_despesas', conn, if_exists='replace', index=False)
 
    return conn


def execute_analytics(conn):
    print("\n>>> Executando Queries Analíticas do arquivo .sql")
    
    if not os.path.exists(SQL_FILE):
        print(f"Erro: Arquivo SQL não encontrado em {SQL_FILE}")
        return

    # QUERY 1: Crescimento
    print("\n--- [Query 1] Top 5 Crescimento de Despesas ---")
    query1 = """
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
    JOIN primeiro_tri p ON u.cnpj = p.cnpj
    ORDER BY crescimento_pct DESC
    LIMIT 5;
    """
    try:
        res1 = pd.read_sql_query(query1, conn)
        print(res1.to_string(index=False, justify='left'))
    except Exception as e:
        print(f"Erro na Query 1: {e}")

    # QUERY 2: Distribuição UF
    print("\n--- [Query 2] Top 5 Estados com Maiores Despesas ---")
    query2 = """
    SELECT 
        o.uf,
        SUM(d.valor_despesa) as total_despesas_estado,
        AVG(d.valor_despesa) as media_por_lancamento,
        COUNT(DISTINCT o.cnpj) as qtd_operadoras
    FROM fact_despesas d
    JOIN dim_operadoras o ON d.cnpj = o.cnpj
    WHERE o.uf != 'Não Informado'
    GROUP BY o.uf
    ORDER BY total_despesas_estado DESC
    LIMIT 5;
    """
    try:
        res2 = pd.read_sql_query(query2, conn)
        pd.options.display.float_format = '{:,.2f}'.format
        print(res2.to_string(index=False, justify='left'))
    except Exception as e:
        print(f"Erro na Query 2: {e}")

   # QUERY 3: Acima da Média
    print("\n--- [Query 3] Operadoras Acima da Média em >= 2 Trimestres ---")
    query3 = """
    WITH media_geral_trimestre AS (
        SELECT data_referencia, AVG(valor_despesa) as media_mercado
        FROM fact_despesas
        GROUP BY data_referencia
    ),
    despesas_operadora AS (
        SELECT cnpj, data_referencia, SUM(valor_despesa) as total_op
        FROM fact_despesas
        GROUP BY 1, 2
    ),
    comparativo AS (
        SELECT 
            d.cnpj,
            CASE WHEN d.total_op > m.media_mercado THEN 1 ELSE 0 END as acima_da_media
        FROM despesas_operadora d
        JOIN media_geral_trimestre m ON d.data_referencia = m.data_referencia
    )
    SELECT 
        o.razao_social,
        SUM(c.acima_da_media) as qtd_trimestres_acima
    FROM comparativo c
    JOIN dim_operadoras o ON c.cnpj = o.cnpj
    GROUP BY o.razao_social
    HAVING SUM(c.acima_da_media) >= 2
    ORDER BY qtd_trimestres_acima DESC
    LIMIT 10;
    """
    try:
        res3 = pd.read_sql_query(query3, conn)
        print(res3.to_string(index=False, justify='left'))
    except Exception as e:
        print(f"Erro na Query 3: {e}")


if __name__ == "__main__":
    connection = create_and_load_db()
    execute_analytics(connection)
    connection.close()
    print("\n>>> Teste de Banco de Dados Finalizado.")