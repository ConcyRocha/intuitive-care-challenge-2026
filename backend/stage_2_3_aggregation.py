import pandas as pd
import os
import zipfile

PROCESSED_DIR = os.path.join("data", "processed")
INPUT_FILE = os.path.join(PROCESSED_DIR, "dados_enriquecido.csv")
OUTPUT_CSV = os.path.join(PROCESSED_DIR, "despesas_agregadas.csv")

FINAL_ZIP = os.path.join(os.getcwd(), "Teste_ConceicaoRocha.zip")


def run_aggregation():
    """
    Executa a etapa final de Agregação Analítica e geração da entrega.

    TRANSFORMAÇÕES REALIZADAS:
    1. Group By: Agrupa os dados transacionais por 'RazaoSocial' e 'UF'.
    2. Engenharia de Features:
       - Total_Despesas: Soma do valor financeiro.
       - Media_Despesas: Média simples por registro.
       - Desvio_Padrao: Mede a volatilidade dos gastos da operadora.
    3. Limpeza Final: Trata desvio padrão nulo (NaN) convertendo para 0.0 
       (caso de operadoras com apenas um lançamento).

    ESTRATÉGIA DE ORDENAÇÃO (TRADE-OFF):
    - Algoritmo: Timsort (implementação padrão do Pandas/NumPy).
    - Complexidade: O(N log N).
    - Justificativa: Como o dataset agregado reduz drasticamente o número de 
      linhas (Cardinalis < 100k), a ordenação em memória (In-Memory Sort) 
      é instantânea e mais eficiente que utilizar índices de banco de dados 
      ou External Merge Sort.

    ENTREGA:
    - Gera o CSV 'despesas_agregadas.csv'.
    - Compacta o arquivo num ZIP 'Teste_ConceicaoRocha.zip' conforme requisito.
    """
    print(">>> Iniciando Etapa 2.3: Agregação e Estatísticas")

    if not os.path.exists(INPUT_FILE):
        print("Erro: Arquivo enriquecido não encontrado. Rode a etapa 2.2.")
        return

    df = pd.read_csv(INPUT_FILE, sep=';', encoding='utf-8')

    df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce').fillna(0.0)

    print(f"   -> Dados carregados: {len(df)} registros. Agrupando...")

    df_agg = df.groupby(['RazaoSocial', 'UF'])['ValorDespesas'].agg(
        Total_Despesas='sum',
        Media_Despesas='mean',
        Desvio_Padrao='std',
        Qtd_Registros='count'
    ).reset_index()

    df_agg['Desvio_Padrao'] = df_agg['Desvio_Padrao'].fillna(0.0)

    cols_float = ['Total_Despesas', 'Media_Despesas', 'Desvio_Padrao']
    df_agg[cols_float] = df_agg[cols_float].round(2)

    print("   -> Ordenando por Total de Despesas (Decrescente)...")
    df_agg = df_agg.sort_values(by='Total_Despesas', ascending=False)

    print(f"   -> Salvando CSV Agregado: {len(df_agg)} linhas.")
    df_agg.to_csv(OUTPUT_CSV, index=False, sep=';', encoding='utf-8')

    print(f"   -> Compactando entrega final: {FINAL_ZIP}")
    with zipfile.ZipFile(FINAL_ZIP, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(OUTPUT_CSV, arcname="despesas_agregadas.csv")

    print(">>> Processo Finalizado com Sucesso!")
    print(f"Arquivo pronto para envio: {FINAL_ZIP}")


if __name__ == "__main__":
    run_aggregation()