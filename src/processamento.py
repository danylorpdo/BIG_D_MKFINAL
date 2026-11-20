"""Integra os CSVs do INPE, verifica a qualidade e publica a camada Silver."""

import hashlib
import json
import os
from fcntl import LOCK_EX, flock
from functools import reduce
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from pyspark.sql import SparkSession, functions as F, types as T

from ingestao import ANOS, BRONZE
from qualidade import (
    COLUNAS_BRONZE, FORMATO_DATA, aparar, ausente, classificar, resumir, verificar_validos,
)

RAIZ = BRONZE.parent.parent
SILVER = RAIZ / "data" / "silver"
RELATORIO = RAIZ / "resultados" / "indicadores" / "qualidade_silver.json"
COLUNAS_SILVER = (*COLUNAS_BRONZE, "ano", "mes", "arquivo_origem", "ano_origem")
TIPOS_SILVER = (
    "bigint", "string", "double", "double", "timestamp", "string", "string", "string",
    "string", "int", "int", "string", "int",
)
CURADORIA_INPE = (
    "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/documentos/"
    "Curadoria-Governanca-Dados-FocosFogoAtivo.pdf"
)
FAQ_INPE = "https://data.inpe.br/queimadas/faq/"


def iniciar_spark(temporario, aplicacao="Queimadas - Silver"):
    return (
        SparkSession.builder.master("local[2]").appName(aplicacao)
        .config("spark.local.dir", str(temporario))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .getOrCreate()
    )


def ler_bronze(spark):
    esquema = T.StructType([T.StructField(nome, T.StringType()) for nome in COLUNAS_BRONZE])
    arquivos = {ano: BRONZE / f"focos_br_ref_{ano}.csv" for ano in ANOS}
    quadros = []
    hashes = {}
    for ano, caminho in arquivos.items():
        with caminho.open("rb") as entrada:
            hashes[caminho.name] = hashlib.file_digest(entrada, "sha256").hexdigest()
        quadro = spark.read.csv(
            str(caminho), schema=esquema, header=True, enforceSchema=False,
            mode="FAILFAST", sep=",", encoding="UTF-8",
        )
        quadros.append(quadro.withColumns({
            "arquivo_origem": F.lit(caminho.name), "ano_origem": F.lit(ano),
        }))
    return reduce(lambda primeiro, segundo: primeiro.unionByName(segundo), quadros), hashes


def padronizar(dados):
    data_texto = aparar(F.col("data_pas"))
    data_formato = data_texto.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    substituicoes = {
        "original": F.struct(*COLUNAS_BRONZE),
        "id_bdq": aparar(F.col("id_bdq")).try_cast("long"),
        "lat": aparar(F.col("lat")).try_cast("double"),
        "lon": aparar(F.col("lon")).try_cast("double"),
        "data_pas": F.when(data_formato, F.try_to_timestamp(data_texto, F.lit(FORMATO_DATA))),
        "foco_id": F.when(~ausente(F.col("foco_id")), aparar(F.col("foco_id"))),
    }
    substituicoes.update({
        nome: F.when(~ausente(F.col(nome)), F.col(nome)) for nome in COLUNAS_BRONZE[5:]
    })
    dados = dados.withColumns(substituicoes)
    return dados.withColumns({"ano": F.year("data_pas"), "mes": F.month("data_pas")})


def contagens_por_origem(dados):
    return {
        (linha.arquivo_origem, linha.ano_origem): linha["count"]
        for linha in dados.groupBy("arquivo_origem", "ano_origem").count().collect()
    }


def validar_gravacao(spark, pasta, resumo):
    focos = spark.read.parquet(str(pasta / "focos"))
    quarentena = spark.read.parquet(str(pasta / "quarentena"))
    if focos.dtypes != list(zip(COLUNAS_SILVER, TIPOS_SILVER)):
        raise RuntimeError("Esquema inesperado na releitura dos focos em Parquet.")
    nulos = verificar_validos(focos)
    if quarentena.where(F.col("motivos").isNull() | (F.size("motivos") == 0)).take(1):
        raise RuntimeError("Registro sem motivo encontrado no Parquet de quarentena.")
    for quadro, campo in ((focos, "validos"), (quarentena, "quarentena")):
        esperado = {
            (linha.arquivo_origem, linha.ano_origem): linha[campo]
            for linha in resumo if linha[campo]
        }
        if contagens_por_origem(quadro) != esperado:
            raise RuntimeError(f"Contagens divergentes na releitura do Parquet de {campo}.")
    return nulos


def publicar(etapa):
    """Só substitui os três resultados após a verificação, com recuperação em caso de erro."""
    destinos = (SILVER / "focos", SILVER / "quarentena", RELATORIO)
    publicar_saidas(etapa, destinos, SILVER)


def publicar_saidas(etapa, destinos, pasta_trava):
    """Publica saídas com nomes distintos preparadas no mesmo sistema de arquivos."""
    for destino in destinos:
        destino.parent.mkdir(parents=True, exist_ok=True)
    anteriores, publicados = [], []
    trava = os.open(pasta_trava, os.O_RDONLY)
    try:
        flock(trava, LOCK_EX)
        for destino in destinos:
            if destino.exists():
                copia = etapa / f".anterior-{destino.name}"
                destino.rename(copia)
                anteriores.append((copia, destino))
        for destino in destinos:
            origem = etapa / destino.name
            origem.rename(destino)
            publicados.append((destino, origem))
    except OSError:
        for destino, origem in reversed(publicados):
            destino.rename(origem)
        for copia, destino in reversed(anteriores):
            copia.rename(destino)
        raise
    finally:
        os.close(trava)


def processar(spark, etapa):
    inicio = perf_counter()
    bronze, hashes = ler_bronze(spark)
    classificados, motivos, entre_arquivos = classificar(padronizar(bronze))
    classificados.persist()
    try:
        resumo = resumir(classificados, motivos, entre_arquivos)
        if {linha.ano_origem for linha in resumo} != set(ANOS):
            raise RuntimeError("Um dos CSVs anuais não retornou registros.")
        tempo_classificacao = perf_counter() - inicio
        validos = classificados.where(F.size("motivos") == 0).select(*COLUNAS_SILVER)
        quarentena = classificados.where(F.size("motivos") > 0).select(
            "arquivo_origem", "ano_origem", "original", "motivos", *entre_arquivos,
        )
        validos.write.parquet(str(etapa / "focos"))
        quarentena.write.parquet(str(etapa / "quarentena"))
    finally:
        classificados.unpersist()

    nulos = validar_gravacao(spark, etapa, resumo)
    totais = {
        nome: sum(linha[nome] for linha in resumo) for nome in ("antes", "validos", "quarentena")
    }
    if totais["antes"] != totais["validos"] + totais["quarentena"]:
        raise RuntimeError("A quantidade de registros não foi conservada.")
    relatorio = {
        "spark": spark.version,
        "fuso_data_pas": "UTC",
        "referencias": [CURADORIA_INPE, FAQ_INPE],
        "bronze_sha256": hashes,
        "totais": totais,
        "por_arquivo": [linha.asDict(recursive=True) for linha in resumo],
        "nulos_por_campo_no_parquet": nulos,
        "regras": {
            "ausentes": "Campos obrigatórios vazios vão para quarentena. Pais, estado, "
            "município e bioma vazios viram nulos e são mantidos se o resto for válido.",
            "invalidos": "Conversões malformadas, coordenadas fora da faixa ou não finitas "
            "e datas fora do ano do CSV vão para quarentena.",
            "duplicados": "Todas as ocorrências de linhas idênticas ou de chaves repetidas "
            "vão para quarentena, inclusive entre arquivos.",
            "normalizacao": "Espaços removidos de lat/lon. Ano e mês derivados em UTC.",
            "contagens": "A contagem por motivo mede linhas. Uma linha pode ter vários motivos.",
        },
        "segundos": {
            "leitura_classificacao_e_resumo": round(tempo_classificacao, 3),
            "gravacao_e_releitura_parquet": round(perf_counter() - inicio - tempo_classificacao, 3),
        },
        "releitura_parquet_validada": True,
    }
    (etapa / RELATORIO.name).write_text(
        json.dumps(relatorio, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    publicar(etapa)
    print(f"Silver | {totais['antes']} lidos | {totais['validos']} válidos | "
          f"{totais['quarentena']} em quarentena | {perf_counter() - inicio:.2f} s")


def main():
    with TemporaryDirectory(prefix=".etapa-silver-", dir=RAIZ) as diretorio:
        etapa = Path(diretorio)
        spark = iniciar_spark(etapa)
        try:
            spark.sparkContext.setLogLevel("ERROR")
            processar(spark, etapa)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
