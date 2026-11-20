# Análise de Focos de Queimadas no Brasil (2024–2025)

Pipeline acadêmico com Python, PySpark e arquitetura Medallion. A Bronze guarda os dados anuais do satélite de referência, a Silver reúne os registros validados e a Gold agrega as contagens de focos. [Fonte oficial do INPE](https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/Brasil_sat_ref/)

## Instalação no Ubuntu

Use Python 3.12 com o módulo `venv` e Java 17. Após clonar o repositório, execute os comandos na raiz do projeto. O PySpark 4.0.1 e o Matplotlib 3.10.7 estão fixados em `requirements.txt`.

```bash
python3 -m venv .venv
mkdir -p .venv/tmp
TMPDIR="$PWD/.venv/tmp" .venv/bin/python -m pip install --no-cache-dir -r requirements.txt
```

Se precisar de Java local, os comandos abaixo instalam o Temurin 17 na pasta do projeto para Linux x86-64. Se já tiver Java 17, aponte `JAVA_HOME` para a instalação existente.

```bash
mkdir -p .venv/java
curl -fL -o .venv/java-temurin.tar.gz \
  https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jre/hotspot/normal/eclipse
tar -xzf .venv/java-temurin.tar.gz --strip-components=1 -C .venv/java
rm .venv/java-temurin.tar.gz
```

Após a instalação, configure o terminal que executará o pipeline. Se o Java estiver fora de `.venv/java/`, defina `JAVA_HOME` para a instalação usada.

```bash
if [ -x .venv/java/bin/java ]; then export JAVA_HOME="$PWD/.venv/java"; fi
export TMPDIR="$PWD/.venv/tmp"
mkdir -p "$TMPDIR"
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$TMPDIR"
export SPARK_LOCAL_IP=127.0.0.1
export MPLCONFIGDIR="$PWD/.venv/matplotlib"
```

## Reprodução completa

A primeira execução da ingestão precisa de acesso ao servidor do INPE para obter os ZIPs de 2024 e 2025. A partir deles, a sequência cria a Bronze, a Silver, a Gold e os gráficos no sistema de arquivos local.

```bash
.venv/bin/python src/ingestao.py &&
.venv/bin/python src/verificar_bronze.py &&
.venv/bin/python src/processamento.py &&
.venv/bin/python src/indicadores.py &&
.venv/bin/python src/visualizacoes.py
```

A ingestão guarda os ZIPs oficiais e extrai os CSVs sem alterar seus bytes. Em novas execuções, reaproveita os ZIPs locais. A verificação compara o cabeçalho e a contagem de cada CSV com seu DataFrame Spark.

## Processamento Silver

Execute o comando abaixo com os CSVs na Bronze e as mesmas variáveis de ambiente da verificação.

```bash
.venv/bin/python src/processamento.py
```

- `data/silver/focos/` contém os registros válidos em Parquet. `data/silver/quarentena/` preserva os campos lidos da Bronze, a origem e os motivos de rejeição.
- `resultados/indicadores/qualidade_silver.json` registra as contagens por arquivo, os problemas, os critérios, as assinaturas SHA-256 dos CSVs e os tempos medidos.
- A execução lê ambos os Parquets e confere as contagens antes de publicar. Uma nova execução substitui as saídas Silver após a verificação.

As coordenadas são convertidas para `double` e verificadas nos intervalos globais de latitude e longitude. `data_pas` segue o horário UTC de aquisição ou passagem do satélite documentado pelo INPE. O processamento grava timestamps como instantes e deriva `ano` e `mes` em UTC. Configure também a sessão Spark de leitura em UTC para reproduzir a exibição e as derivações no mesmo fuso. [Curadoria do INPE](https://dataserver-coids.inpe.br/queimadas/queimadas/focos/documentos/Curadoria-Governanca-Dados-FocosFogoAtivo.pdf), [FAQ do Programa Queimadas](https://data.inpe.br/queimadas/faq/)

A Silver exige `id_bdq`, `foco_id`, `lat`, `lon` e `data_pas`. Campos descritivos em branco viram nulos. As linhas com dados obrigatórios ausentes ou inválidos, ano divergente ou duplicidade vão para a quarentena. Em uma colisão de identificador ou repetição da linha completa, todas as ocorrências são preservadas na quarentena para revisão. A origem de cada registro é preservada pelos campos `arquivo_origem` e `ano_origem`.

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
focos = spark.read.parquet("data/silver/focos")
```

## Indicadores Gold

Execute com a Silver disponível em `data/silver/focos/` e as mesmas variáveis de ambiente usadas nas etapas anteriores.

```bash
.venv/bin/python src/indicadores.py
```

As tabelas ficam em `data/gold/<tabela>/` como Parquet. Cada uma tem um CSV UTF-8 com cabeçalho em `resultados/indicadores/gold_<tabela>.csv`.

| Tabela | Conteúdo |
| --- | --- |
| `focos_anuais` | Contagem por ano e variação percentual em relação ao ano anterior |
| `focos_mensais` | Contagem por ano e mês UTC |
| `focos_por_estado` | Contagem por estado e ano |
| `focos_por_bioma` | Contagem por bioma e ano |
| `ranking_estados` | Ranking anual e do período completo, com ano vazio no escopo `periodo` |
| `destaques_anuais` | Meses, estados e biomas com a maior contagem em cada ano |

O CSV mensal está ordenado por `ano` e `mes`. Ao ler o Parquet no Spark, aplique `.orderBy("ano", "mes")` para obter a mesma ordem. Empates recebem a mesma posição no ranking e todos os líderes empatados aparecem nos destaques. A categoria `Desconhecido` reúne valores nulos ou em branco e o marcador de mesmo nome. Ela entra nos totais por estado e bioma. Apenas estados e biomas identificados concorrem ao ranking e aos destaques de suas respectivas dimensões.

A execução relê os Parquets, verifica os totais anuais e o conteúdo dos CSVs antes de publicar. Os tempos são exibidos no terminal. A variação usa `(focos do ano − focos do ano anterior) / focos do ano anterior × 100` e fica vazia no primeiro ano.

Os indicadores contam focos detectados pelo satélite de referência. Medir área queimada, contar incêndios individuais ou explicar a variação anual exige outras evidências. [FAQ do Programa Queimadas](https://data.inpe.br/queimadas/faq/)

## Visualizações

Com as dependências instaladas e os CSVs `gold_*.csv` em `resultados/indicadores/`, gere as figuras.

```bash
.venv/bin/python src/visualizacoes.py
```

O script usa Matplotlib e grava três PNGs a 200 DPI em `resultados/graficos/`. Ele valida a cobertura mensal, a unicidade das categorias, os totais anuais e o ranking acumulado a partir dos CSVs Gold.

- [Evolução mensal](resultados/graficos/evolucao_mensal.png)
- [Ranking acumulado dos dez estados](resultados/graficos/ranking_estados.png)
- [Comparação entre biomas](resultados/graficos/comparacao_biomas.png)

A Silver contém **414.692 registros válidos** e zero rejeições. Na Gold, são **278.299 focos em 2024** e **136.393 em 2025**, com variação de **−50,99%**. [Totais anuais](resultados/indicadores/gold_focos_anuais.csv)

## Demonstração

Com o ambiente já instalado e a Silver local, execute na raiz do projeto após configurar o terminal.

```bash
.venv/bin/python src/demonstracao.py
cat .demo/indicadores/gold_focos_anuais.csv
cat .demo/indicadores/gold_destaques_anuais.csv
```

O ensaio reexecuta a Gold com PySpark a partir de `data/silver/focos/` e grava os resultados em `.demo/`. Com a Silver e o ambiente já preparados, a demonstração usa apenas arquivos locais.

## Testes

Os testes conferem a reconciliação dos indicadores, os dados das visualizações e a integridade dos PNGs. Podem ser executados com os indicadores e gráficos incluídos no repositório.

```bash
.venv/bin/python -m unittest discover -s tests -v
```
