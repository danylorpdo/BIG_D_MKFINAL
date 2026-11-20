"""Regras de qualidade e métricas auditáveis da transformação Bronze–Silver."""

from pyspark.sql import Window, functions as F

COLUNAS_BRONZE = (
    "id_bdq", "foco_id", "lat", "lon", "data_pas", "pais", "estado", "municipio", "bioma",
)
OBRIGATORIAS = COLUNAS_BRONZE[:5]
CONVERTIDAS = ("id_bdq", "lat", "lon", "data_pas")
CHAVES = ("id_bdq", "foco_id")
LIMITES = {"lat": (-90, 90), "lon": (-180, 180)}
FORMATO_DATA = "yyyy-MM-dd HH:mm:ss"


def aparar(coluna):
    return F.regexp_replace(coluna, r"^\s+|\s+$", "")


def ausente(coluna):
    return coluna.isNull() | (aparar(coluna) == "")


def coordenada_invalida(nome):
    coluna = F.col(nome)
    return coluna.isNotNull() & (F.isnan(coluna) | ~coluna.between(*LIMITES[nome]))


def ano_invalido():
    return F.col("data_pas").isNotNull() & (F.year("data_pas") != F.col("ano_origem"))


def classificar(dados):
    """Todos os motivos coexistem. Chaves repetidas são avaliadas na união completa."""
    motivos = {
        f"ausente_{nome}": ausente(F.col(f"original.{nome}")) for nome in OBRIGATORIAS
    }
    motivos.update({
        f"conversao_invalida_{nome}": (
            ~ausente(F.col(f"original.{nome}")) & F.col(nome).isNull()
        ) for nome in CONVERTIDAS
    })
    motivos.update({
        f"fora_do_intervalo_{nome}": coordenada_invalida(nome) for nome in LIMITES
    })
    motivos["data_fora_do_ano_origem"] = ano_invalido()
    entre_arquivos = {}
    for chave, rotulo in (
        ("original", "duplicata_exata"),
        ("id_bdq", "id_bdq_repetido"),
        ("foco_id", "foco_id_repetido"),
    ):
        janela = Window.partitionBy(chave)
        motivos[rotulo] = F.col(chave).isNotNull() & (F.count("*").over(janela) > 1)
        entre_arquivos[f"{rotulo}_entre_arquivos"] = F.col(chave).isNotNull() & (
            F.min("ano_origem").over(janela) != F.max("ano_origem").over(janela)
        )

    lista_motivos = F.array_compact(F.array(*(
        F.when(condicao, F.lit(nome)) for nome, condicao in motivos.items()
    )))
    dados = dados.withColumns({"motivos": lista_motivos, **entre_arquivos})
    return dados, tuple(motivos), tuple(entre_arquivos)


def resumir(dados, motivos, entre_arquivos):
    """Conta linhas, inclusive quando uma linha tem mais de um motivo."""
    return dados.groupBy("arquivo_origem", "ano_origem").agg(
        F.count("*").alias("antes"),
        F.count_if(F.size("motivos") == 0).alias("validos"),
        F.count_if(F.size("motivos") > 0).alias("quarentena"),
        F.struct(*(
            F.count_if(ausente(F.col(f"original.{nome}"))).alias(nome)
            for nome in COLUNAS_BRONZE
        )).alias("ausentes_antes"),
        F.struct(*(
            F.count_if(F.array_contains("motivos", nome)).alias(nome) for nome in motivos
        )).alias("linhas_por_motivo"),
        F.struct(*(F.count_if(F.col(nome)).alias(nome) for nome in entre_arquivos))
        .alias("linhas_duplicadas_entre_arquivos"),
        F.struct(*(
            F.count_if(F.col(f"original.{nome}") != aparar(F.col(f"original.{nome}"))).alias(nome)
            for nome in LIMITES
        )).alias("coordenadas_com_espacos"),
    ).orderBy("ano_origem").collect()


def verificar_validos(dados):
    """Verifica o Parquet gravado e retorna contagens por origem e nulos restantes."""
    verificacoes = [
        F.count_if(F.col(nome).isNull()).alias(nome) for nome in COLUNAS_BRONZE
    ]
    invalidos = (
        coordenada_invalida("lat") | coordenada_invalida("lon") | ano_invalido()
        | ~F.col("ano").eqNullSafe(F.year("data_pas"))
        | ~F.col("mes").eqNullSafe(F.month("data_pas"))
    )
    totais = dados.agg(
        F.count("*").alias("total"), F.count_if(invalidos).alias("invalidos"),
        *(F.countDistinct(nome).alias(f"distintos_{nome}") for nome in CHAVES),
        F.struct(*verificacoes).alias("nulos"),
    ).first()
    if (
        totais.invalidos or any(totais.nulos[nome] for nome in OBRIGATORIAS)
        or any(totais[f"distintos_{nome}"] != totais.total for nome in CHAVES)
    ):
        raise RuntimeError("O Parquet Silver falhou na verificação de qualidade.")
    return totais.nulos.asDict()
