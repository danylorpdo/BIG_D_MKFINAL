"""Gera e valida contagens de focos detectados na camada Gold."""

import csv
from collections import Counter, defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from pyspark.sql import Window, functions as F

from processamento import RAIZ, SILVER, iniciar_spark, publicar_saidas
from qualidade import aparar

GOLD = RAIZ / "data" / "gold"
INDICADORES = RAIZ / "resultados" / "indicadores"
TOTAIS_ESPERADOS = {2024: 278299, 2025: 136393}
DESCONHECIDO = "Desconhecido"
DIMENSOES = {
    "focos_mensais": "mes",
    "focos_por_estado": "estado",
    "focos_por_bioma": "bioma",
}
ORDENS = {
    "focos_anuais": ("ano",),
    **{nome: ("ano", campo) for nome, campo in DIMENSOES.items()},
    "ranking_estados": ("escopo", "ano", "posicao", "estado"),
    "destaques_anuais": ("ano", "dimensao", "categoria"),
}


def contar(dados, *chaves):
    return dados.groupBy(*chaves).agg(F.count("*").alias("focos"))


def categoria_conhecida(campo):
    texto = aparar(F.col(campo))
    return F.when((texto != "") & (F.lower(texto) != DESCONHECIDO.lower()), texto)


def calcular(silver):
    dados = silver.withColumns({
        campo: F.coalesce(categoria_conhecida(campo), F.lit(DESCONHECIDO))
        for campo in ("estado", "bioma")
    })
    anual = contar(dados, "ano").withColumn("focos_ano_anterior", F.lag("focos").over(
        Window.orderBy("ano")
    )).withColumn("variacao_percentual", F.round(
        100 * F.try_divide(F.col("focos") - F.col("focos_ano_anterior"), F.col("focos_ano_anterior")),
        2,
    ))
    dimensoes = {nome: contar(dados, "ano", campo) for nome, campo in DIMENSOES.items()}
    dimensoes["focos_mensais"] = dimensoes["focos_mensais"].orderBy("ano", "mes")

    estados = dimensoes["focos_por_estado"].where(F.col("estado") != DESCONHECIDO)
    periodo = estados.groupBy("estado").agg(F.sum("focos").alias("focos")).withColumn(
        "ano", F.lit(None).cast("int"),
    )
    ranking = estados.withColumn("escopo", F.lit("anual")).unionByName(
        periodo.withColumn("escopo", F.lit("periodo"))
    ).withColumn(
        "posicao", F.dense_rank().over(Window.partitionBy("escopo", "ano").orderBy(F.desc("focos"))),
    ).select("escopo", "ano", "posicao", "estado", "focos")

    destaques = []
    for tabela, campo in DIMENSOES.items():
        quadro = dimensoes[tabela]
        if campo != "mes":
            quadro = quadro.where(F.col(campo) != DESCONHECIDO)
        categoria = F.format_string("%02d", F.col(campo)) if campo == "mes" else F.col(campo)
        destaques.append(
            quadro.withColumn("_maximo", F.max("focos").over(Window.partitionBy("ano")))
            .where(F.col("focos") == F.col("_maximo"))
            .select("ano", F.lit(campo).alias("dimensao"), categoria.alias("categoria"), "focos")
        )
    return {
        "focos_anuais": anual,
        **dimensoes,
        "ranking_estados": ranking,
        "destaques_anuais": destaques[0].unionByName(destaques[1]).unionByName(destaques[2]),
    }


def verificar(tabelas, total_silver):
    anual = {linha.ano: linha.focos for linha in tabelas["focos_anuais"]}
    if anual != TOTAIS_ESPERADOS or sum(anual.values()) != total_silver:
        raise ValueError(f"A contagem anual não corresponde à Silver: {anual} / {total_silver}")
    for nome, ordem in ORDENS.items():
        linhas = tabelas[nome]
        chaves = [tuple(linha[campo] for campo in ordem if campo != "posicao") for linha in linhas]
        if len(set(chaves)) != len(linhas) or any(linha.focos <= 0 for linha in linhas):
            raise ValueError(f"Chaves duplicadas ou contagens inválidas em {nome}")

    maximos_esperados = set()
    for nome, campo in DIMENSOES.items():
        totais = Counter()
        por_ano = defaultdict(list)
        for linha in tabelas[nome]:
            totais[linha.ano] += linha.focos
            categoria = linha[campo]
            if categoria is None or (campo == "mes" and not 1 <= categoria <= 12):
                raise ValueError(f"Categoria inválida em {nome}: {categoria}")
            if categoria != DESCONHECIDO:
                rotulo = f"{categoria:02d}" if campo == "mes" else categoria
                por_ano[linha.ano].append((rotulo, linha.focos))
        if totais != anual:
            raise ValueError(f"Totais por ano divergentes em {nome}: {totais}")
        for ano, categorias in por_ano.items():
            maximo = max(focos for _, focos in categorias)
            maximos_esperados.update(
                (ano, campo, categoria, focos) for categoria, focos in categorias if focos == maximo
            )
    if {tuple(linha) for linha in tabelas["destaques_anuais"]} != maximos_esperados:
        raise ValueError("Os destaques não correspondem aos máximos das agregações.")

    grupos = defaultdict(Counter)
    for linha in tabelas["focos_por_estado"]:
        if linha.estado != DESCONHECIDO:
            grupos["anual", linha.ano][linha.estado] = linha.focos
            grupos["periodo", None][linha.estado] += linha.focos
    ranking_esperado = set()
    for (escopo, ano), estados in grupos.items():
        posicoes = {focos: pos for pos, focos in enumerate(sorted(set(estados.values()), reverse=True), 1)}
        ranking_esperado.update(
            (escopo, ano, posicoes[focos], estado, focos) for estado, focos in estados.items()
        )
    if {tuple(linha) for linha in tabelas["ranking_estados"]} != ranking_esperado:
        raise ValueError("As posições ou contagens do ranking divergem das agregações por estado.")

    anterior = None
    for linha in tabelas["focos_anuais"]:
        variacao = round(100 * (linha.focos - anterior) / anterior, 2) if anterior else None
        if (linha.focos_ano_anterior, linha.variacao_percentual) != (anterior, variacao):
            raise ValueError("Variação anual incompatível com as contagens.")
        anterior = linha.focos


def exportar_csv(caminho, colunas, linhas):
    # As tabelas agregadas já foram ordenadas e coletadas do Parquet pelo Spark.
    valores = [list(colunas), *(
        ["" if valor is None else str(valor) for valor in linha] for linha in linhas
    )]
    with caminho.open("w", encoding="utf-8", newline="") as arquivo:
        csv.writer(arquivo, lineterminator="\n").writerows(valores)
    with caminho.open(encoding="utf-8", newline="") as arquivo:
        if list(csv.reader(arquivo, strict=True)) != valores:
            raise ValueError(f"O CSV não reproduz o Parquet de {caminho.name}.")


def executar(spark, etapa):
    inicio = perf_counter()
    silver = spark.read.parquet(str(SILVER / "focos")).select("ano", "mes", "estado", "bioma")
    silver.persist()
    try:
        total_silver = silver.count()
        tabelas = calcular(silver)
        for nome, quadro in tabelas.items():
            quadro.write.parquet(str(etapa / nome))
    finally:
        silver.unpersist()
    fim_gravacao = perf_counter()

    gravadas = {nome: spark.read.parquet(str(etapa / nome)) for nome in tabelas}
    linhas = {nome: quadro.orderBy(*ORDENS[nome]).collect() for nome, quadro in gravadas.items()}
    verificar(linhas, total_silver)
    for nome, quadro in gravadas.items():
        exportar_csv(etapa / f"gold_{nome}.csv", quadro.columns, linhas[nome])
    destinos = [GOLD / nome for nome in tabelas]
    destinos.extend(INDICADORES / f"gold_{nome}.csv" for nome in tabelas)
    publicar_saidas(etapa, destinos, GOLD)

    fim = perf_counter()
    print(f"Gold | {total_silver} focos reconciliados | {len(tabelas)} Parquets e CSVs")
    print(f"Leitura, agregação e Parquet: {fim_gravacao - inicio:.2f} s | "
          f"Releitura, validação e CSV: {fim - fim_gravacao:.2f} s | Total: {fim - inicio:.2f} s")


def main():
    with TemporaryDirectory(prefix=".etapa-gold-", dir=RAIZ) as diretorio:
        etapa = Path(diretorio)
        spark = iniciar_spark(etapa, "Queimadas - Gold")
        try:
            spark.sparkContext.setLogLevel("ERROR")
            executar(spark, etapa)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
