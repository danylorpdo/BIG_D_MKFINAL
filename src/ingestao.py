"""Baixa e extrai os CSVs anuais oficiais do INPE na camada Bronze."""

import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen
from zipfile import BadZipFile, ZipFile

ANOS = (2024, 2025)
BRONZE = Path(__file__).resolve().parents[1] / "data" / "bronze"
FONTE = (
    "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/"
    "anual/Brasil_sat_ref/"
)


def ingerir(ano: int) -> None:
    destino_zip = BRONZE / f"focos_br_ref_{ano}.zip"
    arquivo_local = destino_zip.is_file()

    with TemporaryDirectory(prefix=f".ingestao-{ano}-", dir=BRONZE) as diretorio:
        temporario = Path(diretorio)
        origem_zip = destino_zip
        if not arquivo_local:
            origem_zip = temporario / destino_zip.name
            with urlopen(FONTE + destino_zip.name, timeout=60) as resposta:
                with origem_zip.open("wb") as saida:
                    shutil.copyfileobj(resposta, saida)

        with ZipFile(origem_zip) as pacote:
            membros = [
                item for item in pacote.infolist()
                if not item.is_dir() and Path(item.filename).suffix.lower() == ".csv"
            ]
            if len(membros) != 1:
                raise ValueError(f"Esperado um único CSV, encontrados {len(membros)}")

            membro = membros[0]
            nome_csv = Path(membro.filename).name
            if nome_csv != destino_zip.with_suffix(".csv").name:
                raise ValueError(f"Nome inesperado para o CSV: {nome_csv}")
            csv_temporario = temporario / nome_csv
            with pacote.open(membro) as entrada, csv_temporario.open("wb") as saida:
                shutil.copyfileobj(entrada, saida)

        # Publica apenas arquivos completos. A extração até o fim verifica o CRC do CSV.
        if not arquivo_local:
            origem_zip.replace(destino_zip)
        csv_temporario.replace(BRONZE / nome_csv)

    status = "ZIP local reutilizado" if arquivo_local else "ZIP baixado do INPE"
    print(f"{ano} | {status} | {nome_csv}")


def main() -> int:
    BRONZE.mkdir(parents=True, exist_ok=True)
    falhou = False
    for ano in ANOS:
        try:
            ingerir(ano)
        except (OSError, BadZipFile, ValueError) as erro:
            print(f"{ano} | falha na ingestão: {erro}", file=sys.stderr)
            falhou = True
    return int(falhou)


if __name__ == "__main__":
    sys.exit(main())
