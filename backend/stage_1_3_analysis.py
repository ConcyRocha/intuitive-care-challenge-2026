import os
import pandas as pd
import requests
import zipfile
import urllib3

PROCESSED_DIR = os.path.join("data", "processed")
OUTPUT_ZIP = "consolidado_despesas.zip"
INPUT_CSV = os.path.join(PROCESSED_DIR, "despesas_consolidadas.csv")
CADOP_CSV = os.path.join("data", "raw", "Relatorio_cadop.csv")
FINAL_CSV = "consolidado_despesas.csv"
CADOP_BASE = "https://dadosabertos.ans.gov.br/FTP/PDA"
CADOP_URL = (f"{CADOP_BASE}/operadoras_de_plano_de_saude_ativas/"
             f"Relatorio_cadop.csv")


def download_cadop():
    """
    Baixa o Relatório de Cadastros (CADOP).
    Utiliza User-Agent customizado para driblar bloqueios de segurança
    do site da ANS que rejeitam scripts Python (Erro 404/403).
    """
    if os.path.exists(CADOP_CSV):
        print(f"   [Cache] Usando cadastro já baixado: {CADOP_CSV}")
        return

    print("   [Download] Baixando dados cadastrais (CNPJ/Razão Social)...")
    try:
        r = requests.get(CADOP_URL, headers=HEADERS, verify=False, timeout=60)
        r.raise_for_status()

        with open(CADOP_CSV, 'wb') as f:
            f.write(r.content)
        print("   [Sucesso] Download concluído!")

    except Exception as e:
        print(f"   [Erro] Falha ao baixar Cadop: {e}")
        if os.path.exists(CADOP_CSV):
            os.remove(CADOP_CSV)
        raise e


def load_and_enrich_data():
    """
    Carrega dados processados e realiza o Join com a base cadastral.

    TRATATIVA DE INCONSISTÊNCIA DE DATAS:
    - Problema: Arquivos originais possuem nomes variados para data.
    - Abordagem: Extração via Regex (r'(\d)T' e r'(\d{4})') do nome do
      arquivo de origem.
    - Justificativa: Garante que 'Ano' e 'Trimestre' sejam inteiros
      padronizados, independente do formato do nome do arquivo (zip/csv).

    Returns:
        pd.DataFrame: DataFrame unificado contendo chaves para o merge.
    """
    print(">>> Iniciando Etapa 1.3: Análise e Enriquecimento")

    print("   -> Lendo arquivo de despesas consolidadas...")
    df_despesas = pd.read_csv(INPUT_CSV, sep=';', encoding='utf-8')

    df_despesas['Trimestre'] = df_despesas['arquivo_origem'].str.extract(
        r'(\d)T').fillna(0).astype(int)
    df_despesas['Ano'] = df_despesas['arquivo_origem'].str.extract(
        r'(\d{4})').fillna(0).astype(int)

    download_cadop()
    print("   -> Lendo dados cadastrais...")

    try:
        df_cadop = pd.read_csv(CADOP_CSV, sep=';', encoding='latin1',
                               dtype=str)
        if len(df_cadop.columns) < 2:
            raise ValueError("Separador incorreto")
    except Exception:
        print("      [Aviso] Tentando ler Cadop com separador vírgula...")
        df_cadop = pd.read_csv(CADOP_CSV, sep=',', encoding='utf-8',
                               dtype=str)

    df_cadop.columns = [c.strip().upper().replace('"', '')
                        for c in df_cadop.columns]

    possible_names = [
        'REGISTRO_OPERADORA', 'REGISTRO_ANS', 'CD_OPERADORA', 'REG_ANS',
        'REGISTRO ANS'
    ]
    target_col = None

    for nome in possible_names:
        if nome in df_cadop.columns:
            target_col = nome
            break

    if not target_col:
        err_msg = (f"Coluna ANS não encontrada. Colunas: "
                   f"{list(df_cadop.columns)}")
        raise KeyError(err_msg)

    df_cadop = df_cadop.rename(columns={
        target_col: 'reg_ans',
        'RAZAO_SOCIAL': 'RazaoSocial'
    })

    if 'RazaoSocial' not in df_cadop.columns:
        social_col = next((c for c in df_cadop.columns if 'SOCIAL' in c), None)
        if social_col:
            df_cadop = df_cadop.rename(columns={social_col: 'RazaoSocial'})

    df_despesas['reg_ans'] = df_despesas['reg_ans'].astype(str)
    df_cadop['reg_ans'] = df_cadop['reg_ans'].astype(str)

    print("   -> Realizando Enriquecimento (Merge)...")
    cols_to_merge = ['reg_ans', 'CNPJ']
    if 'RazaoSocial' in df_cadop.columns:
        cols_to_merge.append('RazaoSocial')

    df_merged = pd.merge(df_despesas, df_cadop[cols_to_merge],
                         on='reg_ans', how='left')

    return df_merged


def analyze_and_clean(df):
    """
    Aplica regras de negócio e documenta a Análise Crítica dos dados.

    TRATATIVAS DE ANÁLISE CRÍTICA:

    1. CNPJs DUPLICADOS COM RAZÕES SOCIAIS DIFERENTES:
       - Abordagem: Deduplicação baseada no mapeamento '1 CNPJ -> 1 Nome'.
       - Justificativa: Uma operadora pode mudar de nome durante o ano.
         Usamos o nome mais recente/disponível para garantir consistência
         nos agrupamentos por CNPJ.

    2. VALORES ZERADOS OU NEGATIVOS:
       - Zeros: Removidos.
         Justificativa: Não agregam valor contábil (ruído) e incham o dataset.
       - Negativos: Mantidos.
         Justificativa: Em contabilidade, representam estornos ou ajustes
         créditos legítimos. Removê-los geraria erro no saldo final.

    Args:
        df (pd.DataFrame): DataFrame bruto enriquecido.

    Returns:
        pd.DataFrame: DataFrame limpo e validado.
    """
    print("   -> Aplicando regras de análise crítica...")

    initial_rows = len(df)

    if 'cd_conta_contabil' in df.columns:
        df = df[df['cd_conta_contabil'].astype(str).str.startswith('4')]

    if 'CNPJ' in df.columns:
        df_map = df.dropna(subset=['CNPJ']).drop_duplicates('CNPJ')
        cnpj_map = df_map.set_index('CNPJ')['RazaoSocial'].to_dict()
        df['RazaoSocial'] = df['CNPJ'].map(cnpj_map).fillna(df['RazaoSocial'])

    if 'vl_saldo_final' in df.columns:
        df = df[df['vl_saldo_final'] != 0]

    cols_finais = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano',
                   'vl_saldo_final']

    for col in cols_finais:
        if col not in df.columns:
            df[col] = None

    df_final = df[cols_finais].copy()
    df_final.rename(columns={'vl_saldo_final': 'ValorDespesas'}, inplace=True)

    df_final['CNPJ'] = df_final['CNPJ'].fillna('N/A')
    df_final['RazaoSocial'] = df_final['RazaoSocial'].fillna('N/A')

    log_msg = (f"      Linhas originais: {initial_rows} -> "
               f"Após limpeza: {len(df_final)}")
    print(log_msg)

    return df_final


def create_zip_package(df):
    """
    Gera os artefatos finais: CSV consolidado e arquivo ZIP.

    O arquivo final é compactado para otimizar armazenamento e
    atender aos requisitos.
    """
    csv_path = os.path.join(PROCESSED_DIR, FINAL_CSV)

    print(f"   -> Salvando CSV final: {csv_path}")
    df.to_csv(csv_path, index=False, sep=';', encoding='utf-8')

    print(f"   -> Compactando para {OUTPUT_ZIP}...")
    with zipfile.ZipFile(OUTPUT_ZIP, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, arcname=FINAL_CSV)

    print(">>> Sucesso! Arquivo ZIP gerado na raiz do projeto.")


if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        df_enriched = load_and_enrich_data()
        df_clean = analyze_and_clean(df_enriched)
        create_zip_package(df_clean)
    except Exception as e:
        print(f"\n[ERRO FATAL] O script parou: {e}")