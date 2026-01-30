import os
import zipfile
import pandas as pd
import shutil

RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")
TEMP_DIR = os.path.join("data", "temp_extract")


COLUMN_MAPPING = {

    "DATA": "data_referencia",
    "DT_REGISTRO": "data_referencia",
    "ANO_TRIMESTRE": "data_referencia",

    "REG_ANS": "reg_ans",
    "REGISTRO_ANS": "reg_ans",
    "CD_OPERADORA": "reg_ans",

    "CD_CONTA_CONTABIL": "cd_conta_contabil",
    "CONTA": "cd_conta_contabil",

    "DESCRICAO": "descricao",
    "DS_CONTA": "descricao",

    "VL_SALDO_FINAL": "vl_saldo_final",
    "VALOR": "vl_saldo_final",
    "SALDO": "vl_saldo_final"
}

COLUNAS_FINAIS = ["reg_ans", "cd_conta_contabil", "descricao",
                  "vl_saldo_final", "arquivo_origem"]


def clean_currency(value):
    """
    Realiza o parsing de valores monetários no formato brasileiro para float.

    Remove formatações de exibição (pontos de milhar) e ajusta separadores
    decimais.

    Args:
        value (str): O valor original formatado (ex: '1.000,00').

    Returns:
        float: Valor numérico normalizado.
    """
    if isinstance(value, str):
        value = value.replace('.', '').replace(',', '.')
        try:
            return float(value)
        except ValueError:
            return 0.0
    return value


def load_file_content(filepath):
    """
    Carrega dados tabulares abstraindo variações de formato (CSV/Excel).

    Implementa estratégia de fallback para encoding, tentando UTF-8 
    (padrão web) e recorrendo a Latin-1 (padrão legado) para maximizar
    a compatibilidade com arquivos governamentais antigos.

    Args:
        filepath (str): Caminho completo do arquivo extraído.

    Returns:
        pd.DataFrame: DataFrame carregado em memória ou None em caso de erro.
    """
    ext = filepath.split('.')[-1].lower()
    df = None

    try:
        if ext == 'csv' or ext == 'txt':
            try:
                df = pd.read_csv(filepath, sep=';', encoding='latin1',
                                 dtype=str)
            except Exception:
                df = pd.read_csv(filepath, sep=',', encoding='utf-8', 
                                 dtype=str)

        elif ext in ['xlsx', 'xls']:
            df = pd.read_excel(filepath, dtype=str)

    except Exception as e:
        print(f"      [Erro Leitura] {os.path.basename(filepath)}: {e}")
        return None

    return df


def normalize_dataframe(df, filename):
    """
    Padroniza o schema e aplica filtro de negócio (Apenas Despesas).

    Além de renomear colunas, esta função filtra apenas os registros onde a
    Conta Contábil inicia com '4' (Despesas Assistenciais e Administrativas),
    conforme requisito técnico do teste de ignorar outras classes contábeis.

    Args:
        df (pd.DataFrame): DataFrame bruto.
        filename (str): Metadado de origem para rastreabilidade (Lineage).

    Returns:
        pd.DataFrame: DataFrame normalizado e filtrado (ou None se vazio).
    """
    if df is None:
        return None

    df.columns = [c.upper().strip() for c in df.columns]
    df = df.rename(columns=COLUMN_MAPPING)

    if 'cd_conta_contabil' in df.columns:
        df = df[df['cd_conta_contabil'].str.startswith('4', na=False)]

    if df.empty:
        return None

    for col in COLUNAS_FINAIS:
        if col not in df.columns:
            df[col] = None

    df_final = df[COLUNAS_FINAIS].copy()
    df_final['arquivo_origem'] = filename
    df_final['vl_saldo_final'] = df_final[
        'vl_saldo_final'].apply(clean_currency)

    return df_final


def main():
    """
    Orquestrador da Etapa 1.2: ETL com filtro de Despesas.

    Fluxo operacional:
    1. Identificação: Localiza ZIPs brutos baixados.
    2. Extração Controlada: Descompacta em área temporária.
    3. Transformação: Filtra contas de despesa (Classe 4).
    4. Carga: Consolida resultados em CSV único.
    """
    print(">>> Iniciando Etapa 1.2: Processamento e Normalização (ETL)")
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    output_file = os.path.join(PROCESSED_DIR, "despesas_consolidadas.csv")
    zip_files = [f for f in os.listdir(RAW_DIR) if f.endswith('.zip')]

    if not zip_files:
        print("Nenhum arquivo ZIP encontrado. Rode a etapa 1.1 primeiro.")
        return

    first_write = True
    processed_count = 0

    for zip_name in zip_files:
        print(f"\nProcessando ZIP: {zip_name}...")
        zip_path = os.path.join(RAW_DIR, zip_name)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(TEMP_DIR)

            for root, _, files in os.walk(TEMP_DIR):
                for file in files:
                    is_csv = file.lower().endswith('.csv')
                    is_txt = file.lower().endswith('.txt')
                    if not (is_csv or is_txt):
                        continue

                    file_path = os.path.join(root, file)
                    
                    log_msg = (f"   -> Normalizando (Filtrando Classe 4): "
                               f"{file}")
                    print(log_msg)

                    raw_df = load_file_content(file_path)
                    clean_df = normalize_dataframe(raw_df, file)

                    if clean_df is not None and not clean_df.empty:
                        mode = 'w' if first_write else 'a'
                        header = first_write

                        clean_df.to_csv(
                            output_file,
                            mode=mode,
                            header=header,
                            index=False,
                            sep=';',
                            encoding='utf-8')

                        first_write = False
                        processed_count += len(clean_df)
                    else:
                        ignore_msg = ("      [Info] Arquivo ignorado "
                                      "(Sem dados de Despesas).")
                        print(ignore_msg)

        except Exception as e:
            print(f"   [Erro Crítico no ZIP] {zip_name}: {e}")

        shutil.rmtree(TEMP_DIR)
        os.makedirs(TEMP_DIR, exist_ok=True)

    print("\n>>> Sucesso! Processamento concluído.")
    print(f"Total de registros de DESPESAS processados: {processed_count}")
    print(f"Arquivo salvo em: {output_file}")


if __name__ == "__main__":
    main()