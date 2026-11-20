"""Executa a Gold com os dados locais e grava os resultados do ensaio em .demo/."""

import indicadores


def main():
    pasta = indicadores.RAIZ / ".demo"
    indicadores.GOLD = pasta / "gold"
    indicadores.INDICADORES = pasta / "indicadores"
    indicadores.main()
    print("Saídas da demonstração: .demo/gold/ e .demo/indicadores/")


if __name__ == "__main__":
    main()
