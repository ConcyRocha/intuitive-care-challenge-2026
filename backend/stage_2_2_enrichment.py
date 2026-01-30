import pandas as pd
import os
import requests
import urllib3

PROCESSED_DIR = os.path.join("data", "processed")
INPUT_VALID_CSV = os.path.join(PROCESSED_DIR, "despesas_validas.csv")
OUTPUT_FINAL = os.path.join(PROCESSED_DIR, "dados_enriquecido.csv")
RAW_DIR = os.path.join("data", "raw")
CADOP_CSV = os.path.join(RAW_DIR, "Relatorio_cadop.csv")
CADOP_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def download_cadop_if_needed():
    """Baixa o cadastro se não existir (ou garante que está atualizado)."""
    if os.path.exists(CADOP_CSV):
        print(f"   [Cache] Utilizando cadastro local: {CADOP_CSV}")
        return

    print("   [Download] Baixando cadastro atualizado da ANS...")
    urllib3.disable_warnings()
    try:
        r = requests.get(CADOP_URL, headers=HEADERS, verify=False, timeout=60)
        r.raise_for_status()
        with open(CADOP_CSV, 'wb') as f:
            f.write(r.content)
    except Exception as e:
        print(f"   [Erro] Falha no download: {e}")
        raise e


def clean_cadop_dataframe(df):
    """
    Limpa e prepara o dataframe de cadastro para o Join.
    Trata duplicatas e renomeia colunas.
    """
    df.columns = [c.strip().upper().replace('"', '') for c in df.columns]
    
    col_map = {
        'CNPJ': 'CNPJ',
        'UF': 'UF',
        'MODALIDADE': 'Modalidade',
        'REGISTRO_ANS': 'RegistroANS',
        'REGISTROANS': 'RegistroANS'
    }
    
    df = df.rename(columns=col_map)
    
    required = ['CNPJ', 'Modalidade', 'UF']
    available = [c for c in required if c in df.columns]
    
    if 'RegistroANS' not in df.columns:
        possible = ['REG_ANS', 'CD_OPERADORA', 'REGISTRO_OPERADORA']
        for p in possible:
            if p in df.columns:
                df = df.rename(columns={p: 'RegistroANS'})
                break

    cols_final = ['CNPJ', 'RegistroANS', 'Modalidade', 'UF']
    
    cols_final = [c for c in cols_final if c in df.columns]
    
    return df[cols_final]


def run_enrichment():
    """
    Executa o Join entre Despesas Válidas e Cadastro.
    
    ESTRATÉGIA DE JOIN: Left Join
    - Mantemos todas as despesas (lado esquerdo).
    - Trazemos dados do cadastro (lado direito) quando houver match.
    - Se não houver match, preenchemos com 'Não Informado'.
    """
    print(">>> Iniciando Etapa 2.2: Enriquecimento de Dados")

    if not os.path.exists(INPUT_VALID_CSV):
        print("Erro: Arquivo 'despesas_validas.csv' não encontrado. Rode a etapa 2.1 antes.")
        return
        
    df_despesas = pd.read_csv(INPUT_VALID_CSV, sep=';', encoding='utf-8', dtype=str)
    print(f"   -> Despesas carregadas: {len(df_despesas)} registros")

    download_cadop_if_needed()
    
    try:
        df_cadop = pd.read_csv(CADOP_CSV, sep=';', encoding='latin1', dtype=str)
    except:
        df_cadop = pd.read_csv(CADOP_CSV, sep=';', encoding='utf-8', dtype=str)

    df_cadop = clean_cadop_dataframe(df_cadop)
    

    initial_cadop = len(df_cadop)
    df_cadop = df_cadop.drop_duplicates(subset=['CNPJ'], keep='first')
    print(f"   -> Cadastro carregado: {initial_cadop} operadoras (Unique: {len(df_cadop)})")

    print("   -> Realizando Left Join via CNPJ...")
    
    cols_to_use = df_cadop.columns.difference(df_despesas.columns).tolist()
    cols_to_use.append('CNPJ')
    
    df_final = pd.merge(
        df_despesas, 
        df_cadop[cols_to_use], 
        on='CNPJ', 
        how='left'
    )


    cols_enrich = ['Modalidade', 'UF']
    for col in cols_enrich:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna('Não Informado')

    print(f"   -> Salvando arquivo final: {OUTPUT_FINAL}")
    df_final.to_csv(OUTPUT_FINAL, index=False, sep=';', encoding='utf-8')
    print(">>> Concluído com sucesso.")


if __name__ == "__main__":
    run_enrichment()