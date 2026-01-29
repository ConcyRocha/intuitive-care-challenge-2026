import unittest
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from stage_1_2_processing import clean_currency, normalize_dataframe  # noqa: E402


class TestDataCleaning(unittest.TestCase):

    def test_clean_currency_br_format(self):
        """Testa se valores em formato BRL (1.000,00) viram float corretamente."""
        self.assertEqual(clean_currency("1.000,00"), 1000.0)
        self.assertEqual(clean_currency("50,55"), 50.55)
        self.assertEqual(clean_currency("0,00"), 0.0)
    
    def test_clean_currency_dirty_input(self):
        """Testa resiliência contra sujeira (espaços, nulos)."""
        self.assertEqual(clean_currency(""), 0.0)
        self.assertEqual(clean_currency(None), None)
        
    def test_normalization_columns(self):
        """Testa se as colunas estranhas são renomeadas para o padrão oficial."""
        data = {
            'DATA': ['2025-01-01'],
            'CD_CONTA_CONTABIL': ['1'],
            'VL_SALDO_FINAL': ['100,00'],
            'REG_ANS': ['123456']
        }
        df_dirty = pd.DataFrame(data)

        df_clean = normalize_dataframe(df_dirty, "arquivo_teste.csv")

        expected_cols = ["reg_ans", "cd_conta_contabil", "descricao", "vl_saldo_final", "arquivo_origem"]
        for col in expected_cols:
            self.assertIn(col, df_clean.columns)

        self.assertEqual(df_clean.iloc[0]['vl_saldo_final'], 100.0)


if __name__ == '__main__':
    unittest.main()