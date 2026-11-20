"""Inspeciona os CSVs da Bronze e confere a leitura por DataFrames Spark."""

import csv
from collections import Counter
from datetime import datetime
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from ingestao import ANOS, BRONZE


def inspecionar_csv(caminho, ano):
    vazios = Counter()
    espacos = Counter()
    chaves = {"id_bdq": set(), "foco_id": set()}
    chaves_repetidas = Counter()
    linhas = set()
    duplicatas = 0
    menor_data, maior_data = datetime.max, datetime.min
    datas_fora_do_ano = 0
    coordenadas_invalidas = 0

    with caminho.open(encoding="utf-8", newline="") as arquivo:
        leitor = csv.reader(arquivo, strict=True)
        colunas = next(leitor)
        registros = 0

        for registros, valores in enumerate(leitor, start=1):
            if len(valores) != len(colunas):
                raise ValueError(f"{caminho.name}: número de campos inválido no registro {registros}")
            linha = tuple(valores)
            duplicatas += linha in linhas
            linhas.add(linha)
            dados = dict(zip(colunas, valores))
            vazios.update(coluna for coluna, valor in dados.items() if not valor.strip())
            espacos.update(coluna for coluna, valor in dados.items() if valor != valor.strip())
            for coluna, vistos in chaves.items():
                valor = dados[coluna]
                if valor.strip():
                    chaves_repetidas[coluna] += valor in vistos
                    vistos.add(valor)
            if dados["data_pas"].strip():
                data = datetime.strptime(dados["data_pas"], "%Y-%m-%d %H:%M:%S")
                menor_data = min(data, menor_data)
                maior_data = max(data, maior_data)
                datas_fora_do_ano += data.year != ano
            if dados["lat"].strip() and dados["lon"].strip():
                latitude, longitude = float(dados["lat"]), float(dados["lon"])
                coordenadas_invalidas += not (-90 <= latitude <= 90 and -180 <= longitude <= 180)

        print(f"{ano} | CSV | {registros} registros | quebras={arquivo.newlines!r}")
    print(f"  Campos vazios: {dict(vazios)} | duplicatas exatas: {duplicatas}")
    print(f"  Campos com espaços nas bordas: {dict(espacos)}")
    print(f"  Chaves repetidas além da primeira ocorrência: {dict(chaves_repetidas)}")
    print(f"  Datas: {menor_data} a {maior_data} | fora do ano: {datas_fora_do_ano}")
    print(f"  Coordenadas fora dos limites globais: {coordenadas_invalidas}")
    return colunas, registros


def main():
    caminhos = {ano: BRONZE / f"focos_br_ref_{ano}.csv" for ano in ANOS}
    perfis = {ano: inspecionar_csv(caminho, ano) for ano, caminho in caminhos.items()}

    with TemporaryDirectory(prefix=".spark-", dir=BRONZE.parent.parent) as temporario:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("Queimadas - leitura Bronze")
            .config("spark.local.dir", temporario)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
            .config("spark.sql.csv.parser.columnPruning.enabled", "false")
            .getOrCreate()
        )
        try:
            spark.sparkContext.setLogLevel("ERROR")
            for ano, caminho in caminhos.items():
                dados = spark.read.csv(
                    str(caminho), header=True, inferSchema=True, mode="FAILFAST",
                    sep=",", encoding="UTF-8",
                )
                registros = dados.count()
                if (dados.columns, registros) != perfis[ano]:
                    raise ValueError(f"{ano}: cabeçalho ou contagem divergente no Spark")
                print(f"{ano} | Spark {spark.version} | {registros} registros | {dados.dtypes}")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
