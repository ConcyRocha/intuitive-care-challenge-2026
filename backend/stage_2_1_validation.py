import pandas as pd
import os
import re


PROCESSED_DIR = os.path.join("data", "processed")
INPUT_CSV = os.path.join(PROCESSED_DIR, "consolidado_despesas.csv")
OUTPUT_VALID = os.path.join(PROCESSED_DIR, "despesas_validas.csv")
OUTPUT_INVALID = os.path.join(PROCESSED_DIR, "despesas_rejeitadas.csv")


def validate_cnpj_math(cnpj):
    """
    Valida os dígitos verificadores de um CNPJ segundo a regra da Receita 
    Federal.

    Algoritmo:
    1. Calcula o primeiro dígito verificador usando pesos [5,4,3,2,9,8,7,6,5,4,3,2].
    2. Calcula o segundo dígito usando o primeiro resultado.
    3. Compara com os dígitos originais.
    """
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    
    soma = sum(int(cnpj[i]) * weights[i] for i in range(12))
    remainder = soma % 11
    digit_1 = 0 if remainder < 2 else 11 - remainder

    weights.insert(0, 6)
    soma = sum(int(cnpj[i]) * weights[i] for i in range(13))
    remainder = soma % 11
    digit_2 = 0 if remainder < 2 else 11 - remainder

    return cnpj[-2:] == f"{digit_1}{digit_2}"


def run_validation_pipeline():
    """
    Executa o pipeline de Qualidade de Dados (Data Quality).

    REGRAS DE VALIDAÇÃO APLICADAS:
    1. Completude: Razão Social não pode ser vazia ou nula.
    2. Consistência Financeira: Valores de despesas devem ser estritamente
       positivos (> 0). Valores zerados ou negativos são segregados.
    3. Integridade Fiscal: O CNPJ deve ser matematicamente válido.

    ESTRATÉGIA DE TRATAMENTO DE ERROS (TRADE-OFF):
    - Abordagem: Segregação (Valid & Invalid Sinks).
    - Justificativa: Ao invés de descartar registros com CNPJ inválido ou
      valores negativos (que podem ser estornos legítimos), salvamos em
      'despesas_rejeitadas.csv' para permitir auditoria e correção posterior
      sem interromper o fluxo principal.
    """
    print(">>> Iniciando Etapa 2.1: Validação e Qualidade de Dados")

    if not os.path.exists(INPUT_CSV):
        print(f"Erro: Arquivo de entrada não encontrado: {INPUT_CSV}")
        return

    df = pd.read_csv(INPUT_CSV, sep=';', encoding='utf-8', dtype=str)
    print(f"   -> Total de registros carregados: {len(df)}")

    df['ValorDespesas'] = pd.to_numeric(
        df['ValorDespesas'], errors='coerce'
    ).fillna(0.0)

    mask_razao = df['RazaoSocial'].notna() & (df['RazaoSocial'].str.strip() != '') & (df['RazaoSocial'] != 'N/A')

    mask_valor = df['ValorDespesas'] > 0

    print("   -> Validando dígitos verificadores de CNPJ...")
    mask_cnpj = df['CNPJ'].apply(validate_cnpj_math)

    df['is_valid'] = mask_razao & mask_valor & mask_cnpj

    df.loc[~mask_razao, 'motivo_rejeicao'] = 'Razão Social Vazia'
    df.loc[~mask_valor, 'motivo_rejeicao'] = 'Valor Zerado ou Negativo'
    df.loc[~mask_cnpj, 'motivo_rejeicao'] = 'CNPJ Inválido'

    df_valid = df[df['is_valid']].copy().drop(
        columns=['is_valid','motivo_rejeicao']
    )

    df_invalid = df[~df['is_valid']].copy().drop(columns=['is_valid'])

    print(f"   -> Salvando Válidos: {len(df_valid)} registros")
    df_valid.to_csv(OUTPUT_VALID, index=False, sep=';', encoding='utf-8')

    print(f"   -> Salvando Rejeitados: {len(df_invalid)} registros")
    df_invalid.to_csv(OUTPUT_INVALID, index=False, sep=';', encoding='utf-8')

    print(">>> Validação concluída com sucesso.")


if __name__ == "__main__":
    run_validation_pipeline()