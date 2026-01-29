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

COLUNAS_FINAIS = ["reg_ans", "cd_conta_contabil", "descricao", "vl_saldo_final", "arquivo_origem"]


def clean_currency(value):
    """Transforma strings de dinheiro (1.000,00) em float (1000.0)."""
    if isinstance(value, str):
        value = value.replace('.', '').replace(',', '.')
        try:
            return float(value)
        except ValueError:
            return 0.0
    return value


def load_file_content(filepath):
    """
    Tenta carregar CSV (com separadores variados) ou Excel.
    Retorna um DataFrame ou None.
    """
    ext = filepath.split('.')[-1].lower()
    df = None

    try:
        if ext == 'csv' or ext == 'txt':
            try:
                df = pd.read_csv(filepath, sep=';', encoding='latin1', dtype=str)
            except Exception:
                df = pd.read_csv(filepath, sep=',', encoding='utf-8', dtype=str)

        elif ext in ['xlsx', 'xls']:
            df = pd.read_excel(filepath, dtype=str)

    except Exception as e:
        print(f"      [Erro Leitura] {os.path.basename(filepath)}: {e}")
        return None

    return df


def normalize_dataframe(df, filename):
    """Aplica a padronização de colunas e limpeza de tipos."""
    if df is None:
        return None

    df.columns = [c.upper().strip() for c in df.columns]

    df = df.rename(columns=COLUMN_MAPPING)

    for col in COLUNAS_FINAIS:
        if col not in df.columns:
            df[col] = None      
    df_final = df[COLUNAS_FINAIS].copy()
    df_final['arquivo_origem'] = filename  
    df_final['vl_saldo_final'] = df_final[
        'vl_saldo_final'].apply(clean_currency)  
    return df_final


def main():
    print(">>> Iniciando Etapa 1.2: Processamento e Normalização (ETL)") 
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_file = os.path.join(PROCESSED_DIR, "despesas_consolidadas.csv") 
    zip_files = [f for f in os.listdir(RAW_DIR) if f.endswith('.zip')]
    if not zip_files:
        print("Nenhum arquivo ZIP encontrado em data/raw/. Rode a etapa 1.1 primeiro.")
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
                    if not (file.lower().endswith('.csv') or file.lower().endswith('.txt')):
                        continue                 
                    file_path = os.path.join(root, file)
                    print(f"   -> Normalizando: {file}")

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

        except Exception as e:
            print(f"   [Erro Crítico no ZIP] {zip_name}: {e}")

        shutil.rmtree(TEMP_DIR)
        os.makedirs(TEMP_DIR, exist_ok=True)

    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)

    print("\n>>>Sucesso! Processamento concluído.")
    print(f"Total de registros processados: {processed_count}")
    print(f"Arquivo salvo em: {output_file}")


if __name__ == "__main__":
    main()