"""Verifica os indicadores e gráficos publicados a partir da camada Gold."""

import csv
import unittest
from collections import Counter
from pathlib import Path

from PIL import Image

from src.visualizacoes import carregar_dados


RESULTADOS = Path(__file__).resolve().parents[1] / "resultados"
TOTAIS_ANUAIS = {2024: 278299, 2025: 136393}


def ler_gold(tabela):
    caminho = RESULTADOS / "indicadores" / f"gold_{tabela}.csv"
    with caminho.open(encoding="utf-8", newline="") as arquivo:
        return list(csv.DictReader(arquivo, strict=True))


class ResultadosGoldTest(unittest.TestCase):
    def test_totais_e_variacao_anuais(self):
        anuais = ler_gold("focos_anuais")
        self.assertEqual([int(linha["ano"]) for linha in anuais], [2024, 2025])
        self.assertEqual({int(l["ano"]): int(l["focos"]) for l in anuais}, TOTAIS_ANUAIS)
        self.assertEqual(anuais[0]["variacao_percentual"], "")
        self.assertEqual(anuais[1]["focos_ano_anterior"], anuais[0]["focos"])
        self.assertEqual(anuais[1]["variacao_percentual"], "-50.99")

    def test_reconciliacao_dos_indicadores(self):
        for tabela, dimensao in (
            ("focos_mensais", "mes"), ("focos_por_estado", "estado"), ("focos_por_bioma", "bioma"),
        ):
            with self.subTest(tabela=tabela):
                linhas = ler_gold(tabela)
                chaves = {(linha["ano"], linha[dimensao]) for linha in linhas}
                self.assertEqual(len(chaves), len(linhas))
                totais = Counter()
                for linha in linhas:
                    totais[int(linha["ano"])] += int(linha["focos"])
                self.assertEqual(totais, TOTAIS_ANUAIS)

    def test_dados_das_visualizacoes(self):
        meses, estados, biomas, series_biomas = carregar_dados()
        self.assertEqual({ano: len(serie) for ano, serie in meses.items()}, {2024: 12, 2025: 12})
        self.assertEqual((meses[2024][8], meses[2025][9]), (83154, 28678))
        self.assertEqual(len({estado for estado, _ in estados}), 10)
        self.assertEqual(estados[0], ("PARÁ", 74437))
        self.assertEqual(len(set(biomas)), 6)
        self.assertEqual({ano: sum(serie) for ano, serie in series_biomas.items()}, TOTAIS_ANUAIS)

    def test_integridade_dos_pngs(self):
        for nome in ("evolucao_mensal", "ranking_estados", "comparacao_biomas"):
            with self.subTest(figura=nome):
                caminho = RESULTADOS / "graficos" / f"{nome}.png"
                with Image.open(caminho) as imagem:
                    self.assertEqual(imagem.format, "PNG")
                    self.assertGreaterEqual(min(imagem.info["dpi"]), 150)
                    imagem.verify()


if __name__ == "__main__":
    unittest.main()
