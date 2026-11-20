"""Gera figuras estáticas a partir dos CSVs Gold de focos detectados."""

import csv
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

import matplotlib

matplotlib.use("Agg")
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator

RAIZ = Path(__file__).resolve().parents[1]
INDICADORES = RAIZ / "resultados" / "indicadores"
GRAFICOS = RAIZ / "resultados" / "graficos"
ANOS = (2024, 2025)
MESES = ("Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez")
DPI = 200
CORES = {2024: "#175A96", 2025: "#B34819"}
TEXTO = "#182635"
MUTED = "#455568"
FONTE = "Fonte dos dados · INPE / Programa Queimadas, satélite de referência (camada Gold)"


def ler_contagens(tabela, *campos, escopo=None):
    arquivo = INDICADORES / f"gold_{tabela}.csv"
    contagens = {}
    chaves = set()
    with arquivo.open(encoding="utf-8", newline="") as origem:
        for linha in csv.DictReader(origem, strict=True):
            if escopo is not None and linha["escopo"] != escopo:
                continue
            chave = tuple(
                int(linha[campo]) if campo in ("ano", "mes") else linha[campo].strip()
                for campo in campos
            )
            canonica = tuple(valor.casefold() if isinstance(valor, str) else valor for valor in chave)
            focos = int(linha["focos"])
            if canonica in chaves or "" in chave or focos < 0:
                raise ValueError(f"{arquivo.name}: categoria repetida, vazia ou contagem inválida: {chave}")
            chaves.add(canonica)
            contagens[chave] = focos
    if not contagens:
        raise ValueError(f"{arquivo.name}: seleção sem registros")
    return contagens


def carregar_dados():
    anuais = ler_contagens("focos_anuais", "ano")
    mensais = ler_contagens("focos_mensais", "ano", "mes")
    biomas = ler_contagens("focos_por_bioma", "ano", "bioma")
    estados = ler_contagens("focos_por_estado", "ano", "estado")
    ranking = ler_contagens("ranking_estados", "estado", escopo="periodo")

    meses = range(1, len(MESES) + 1)
    if set(anuais) != {(ano,) for ano in ANOS} or any(not anuais[ano,] for ano in ANOS):
        raise ValueError("Totais anuais ausentes, vazios ou com anos inesperados")
    if set(mensais) != {(ano, mes) for ano in ANOS for mes in meses}:
        raise ValueError("O CSV mensal deve conter janeiro a dezembro de cada ano")
    categorias = {bioma for _, bioma in biomas}
    if set(biomas) != {(ano, bioma) for ano in ANOS for bioma in categorias}:
        raise ValueError("Os biomas devem ser comparáveis nos dois anos")
    for nome, dados in (("meses", mensais), ("biomas", biomas), ("estados", estados)):
        totais = Counter()
        for (ano, _), focos in dados.items():
            totais[ano,] += focos
        if totais != anuais:
            raise ValueError(f"As contagens por {nome} divergem dos totais anuais")

    acumulado = Counter()
    for (_, estado), focos in estados.items():
        if estado != "Desconhecido":
            acumulado[estado,] += focos
    if ranking != acumulado or len(ranking) < 10:
        raise ValueError("O ranking do período diverge das contagens estaduais ou tem menos de 10 estados")

    series_mensais = {ano: [mensais[ano, mes] for mes in meses] for ano in ANOS}
    ordem_biomas = sorted(categorias, key=lambda nome: (-sum(biomas[a, nome] for a in ANOS), nome))
    series_biomas = {ano: [biomas[ano, nome] for nome in ordem_biomas] for ano in ANOS}
    dez_estados = sorted(
        ((estado, focos) for (estado,), focos in ranking.items()),
        key=lambda registro: (-registro[1], registro[0]),
    )[:10]
    return series_mensais, dez_estados, ordem_biomas, series_biomas


def inteiro(valor):
    return f"{valor:,.0f}".replace(",", ".")


def painel(titulo, subtitulo, tamanho, esquerda=0.12):
    fig, ax = plt.subplots(figsize=tamanho, facecolor="white")
    fig.subplots_adjust(left=esquerda, right=0.96, top=0.79, bottom=0.19)
    fig.text(esquerda, 0.925, titulo, fontsize=19, fontweight="bold", color=TEXTO)
    fig.text(esquerda, 0.865, subtitulo, fontsize=10.5, color=MUTED)
    fig.text(esquerda, 0.047, FONTE, fontsize=8.5, color=MUTED)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color("#BCC7D1")
    ax.tick_params(length=0, pad=6, labelsize=10, colors=TEXTO)
    ax.set_axisbelow(True)
    return fig, ax


def eixo_contagens(ax, direcao):
    eixo = ax.yaxis if direcao == "y" else ax.xaxis
    eixo.set_major_locator(MaxNLocator(nbins=6, integer=True))
    eixo.set_major_formatter(FuncFormatter(lambda valor, _: inteiro(valor)))
    ax.grid(axis=direcao, color="#DEE5EC", linewidth=0.8)
    eixo.labelpad = 9
    eixo.set_label_text("Focos detectados", color=TEXTO)


def evolucao_mensal(series):
    fig, ax = painel("Evolução mensal dos focos", "Brasil · 2024 e 2025", (10, 5.6))
    meses = range(1, len(MESES) + 1)
    for ano, marcador, traco, ajuste in (
        (2024, "o", "-", 12), (2025, "s", "--", -19),
    ):
        valores = series[ano]
        ax.plot(meses, valores, label=str(ano), color=CORES[ano], marker=marcador,
                linestyle=traco, linewidth=2.15, markersize=4.8)
        pico = max(valores)
        for mes, valor in zip(meses, valores):
            if valor == pico:
                ax.scatter(mes, pico, s=85, marker=marcador, color=CORES[ano],
                           edgecolor="white", linewidth=1.35, zorder=4)
                ax.annotate(inteiro(pico), (mes, pico), xytext=(0, ajuste),
                            textcoords="offset points", ha="center", color=CORES[ano],
                            fontsize=10, fontweight="bold")
    ax.set(ylim=(0, max(map(max, series.values())) * 1.16), xlim=(0.7, 12.3))
    ax.set_xticks(meses, MESES)
    ax.set_xlabel("Mês (UTC)", color=TEXTO, labelpad=9)
    eixo_contagens(ax, "y")
    ax.legend(loc="upper left", frameon=False, ncols=2)
    return fig


def nome_estado(estado):
    preposicoes = {"da", "de", "do", "das", "dos"}
    return " ".join(palavra.lower() if palavra.lower() in preposicoes else palavra.capitalize()
                    for palavra in estado.split())


def ranking_estados(estados):
    fig, ax = painel("Estados com mais focos detectados", "10 maiores contagens · 2024–2025",
                    (9.6, 6.8), esquerda=0.24)
    nomes = [nome_estado(estado) for estado, _ in estados]
    valores = [focos for _, focos in estados]
    barras = ax.barh(nomes, valores, height=0.64, color="#35687A")
    ax.invert_yaxis()
    ax.set_xlim(0, max(valores) * 1.20)
    eixo_contagens(ax, "x")
    ax.bar_label(barras, labels=[inteiro(valor) for valor in valores], padding=6,
                 fontsize=10, color=TEXTO)
    return fig


def comparacao_biomas(biomas, series):
    fig, ax = painel("Focos detectados por bioma", "Brasil · comparação entre 2024 e 2025", (10.4, 5.6))
    posicoes = range(len(biomas))
    largura = 0.34
    for ano, fator in ((2024, -0.5), (2025, 0.5)):
        barras = ax.bar([posicao + fator * largura for posicao in posicoes], series[ano],
                        width=largura, label=str(ano), color=CORES[ano])
        ax.bar_label(barras, labels=[inteiro(valor) for valor in series[ano]], padding=3,
                     fontsize=8.2, color=TEXTO)
    ax.set_xticks(posicoes, biomas)
    ax.set_xlabel("Bioma", color=TEXTO, labelpad=9)
    ax.set_ylim(0, max(map(max, series.values())) * 1.16)
    eixo_contagens(ax, "y")
    ax.legend(loc="upper right", frameon=False, ncols=2)
    return fig


def main():
    mensais, estados, biomas, series_biomas = carregar_dados()
    GRAFICOS.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".figuras-", dir=GRAFICOS) as diretorio:
        temporario = Path(diretorio)
        figuras = (
            ("evolucao_mensal.png", evolucao_mensal, (mensais,)),
            ("ranking_estados.png", ranking_estados, (estados,)),
            ("comparacao_biomas.png", comparacao_biomas, (biomas, series_biomas)),
        )
        for nome, desenhar, argumentos in figuras:
            fig = desenhar(*argumentos)
            try:
                fig.savefig(temporario / nome, dpi=DPI, facecolor="white")
            finally:
                plt.close(fig)
        for nome, _, _ in figuras:
            (temporario / nome).replace(GRAFICOS / nome)
    print(f"3 figuras geradas a {DPI} DPI a partir dos CSVs Gold validados.")


if __name__ == "__main__":
    main()
