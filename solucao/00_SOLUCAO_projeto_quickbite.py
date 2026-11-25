# Databricks notebook source

# MAGIC %md
# MAGIC # Projeto final · QuickBite (SOLUÇÃO)
# MAGIC **Do arquivo que chega ao app: landing → pipeline (bronze, silver, gold) → job com gatilho → Databricks App**
# MAGIC
# MAGIC > **Versão do instrutor.** Traz a solução completa (pipeline em SQL e em Python, job, app) e os números esperados. Cada decisão tem alternativas válidas.
# MAGIC
# MAGIC ## O caso
# MAGIC A **QuickBite** é uma plataforma de delivery (Lisboa, Coimbra, Braga). Os arquivos chegam aos poucos: pedidos (JSON), avaliações, restaurantes e entregadores (CSV). A diretoria quer um painel sempre atualizado para responder:
# MAGIC
# MAGIC | Área | Pergunta |
# MAGIC |---|---|
# MAGIC | Operação | Quantos pedidos, quanto faturamos e quanto perdemos com cancelamentos? Em que horários a demanda aperta? |
# MAGIC | Logística | Estamos entregando no prazo? Quem são os entregadores e veículos mais rápidos? |
# MAGIC | Restaurantes | Quais vendem mais, cancelam mais, são mais bem avaliados? Que pratos puxam a receita? |
# MAGIC | Experiência | Como estão as avaliações? Quais os motivos de nota baixa? |
# MAGIC | Dados | Podemos confiar nesses números? Quanto lixo havia nos arquivos? |
# MAGIC
# MAGIC ## A arquitetura que vamos construir

# COMMAND ----------

import os
try:
    _svg = open("../docs/arquitetura.svg", encoding="utf-8").read()
    displayHTML(_svg)
except Exception as e:
    print("Não consegui abrir ../docs/arquitetura.svg (rode o notebook de dentro do Git folder). Veja o diagrama no README do repositório.", e)

# COMMAND ----------

# MAGIC %md
# MAGIC | Peça | O que faz |
# MAGIC |---|---|
# MAGIC | Volume **landing** | Onde os arquivos chegam. Um **simulador** (notebook de apoio) copia um lote por vez |
# MAGIC | **Pipeline** declarativo | Bronze e silver com **streaming tables**, gold com **materialized views**, quarentena e relatório de qualidade |
# MAGIC | **Job** | **Gatilho de chegada de arquivo** no landing, tarefa 1 = pipeline, tarefa 2 = atualizar o app |
# MAGIC | **Databricks App** | Painel que lê só a camada gold |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

import os, glob
from pyspark.sql import functions as F

try:
    usuario = spark.sql("SELECT current_user()").first()[0]
except Exception:
    usuario = "aluno"

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema", "quickbite", "Schema (volumes e tabelas)")
dbutils.widgets.text("volume_origem", "quickbite_files", "Volume com os arquivos de origem")
dbutils.widgets.text("volume_landing", "landing", "Volume de landing")
dbutils.widgets.text("sla_minutos", "45", "SLA de entrega (minutos)")
dbutils.widgets.dropdown("linguagem", "sql", ["sql", "python"], "Linguagem do pipeline")

catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()
ORIGEM = f"/Volumes/{catalogo}/{schema}/{dbutils.widgets.get('volume_origem').strip()}"
LANDING = f"/Volumes/{catalogo}/{schema}/{dbutils.widgets.get('volume_landing').strip()}"
SLA_MIN = int(dbutils.widgets.get("sla_minutos"))
LINGUAGEM = dbutils.widgets.get("linguagem")

nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
RAIZ_REPO = os.path.dirname(os.path.dirname(nb))   # pasta do Git folder no workspace
print("origem :", ORIGEM); print("landing:", LANDING); print("repo   :", RAIZ_REPO)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalogo}`.`{schema}`")

# o volume de landing e as pastas precisam existir antes do pipeline
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalogo}`.`{schema}`.`{dbutils.widgets.get('volume_landing').strip()}`")
for _p in ["orders", "ratings", "restaurants", "couriers"]:
    os.makedirs(f"{LANDING}/{_p}", exist_ok=True)
display(dbutils.fs.ls(ORIGEM))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explorar os arquivos de origem
# MAGIC Antes de qualquer pipeline, **leia os dados crus e investigue**. Não existe uma lista de problemas pronta: montem a de vocês.
# MAGIC
# MAGIC **Perguntas para guiar** (use `spark.read` direto no volume de origem, sem salvar nada):
# MAGIC - Quantas linhas há em cada fonte? Quantos pedidos *distintos*?
# MAGIC - Cada campo tem **um único formato**? Olhem preços, datas, cidades e e-mails.
# MAGIC - Onde há nulos, e **em que situação** (status do pedido)?
# MAGIC - Os identificadores batem entre os arquivos? (restaurantes, entregadores, pedidos avaliados)
# MAGIC - Os valores têm faixas plausíveis? Há algo que não parece cliente real?
# MAGIC - Os arquivos se sobrepõem? O mesmo pedido pode aparecer mais de uma vez?
# MAGIC
# MAGIC **Anotem** o catálogo de problemas (problema · onde · quantas linhas · o que decidem fazer: corrigir, descartar, quarentena...).

# COMMAND ----------

ped = spark.read.json(f"{ORIGEM}/orders_*.json")
print("linhas:", ped.count(), "| pedidos distintos:", ped.select("order_id").distinct().count())
display(ped.groupBy("order_id").count().filter("count > 1").groupBy("count").count())
display(ped.groupBy("status").agg(F.count("*").alias("linhas"), F.sum(F.col("courier_id").isNull().cast("int")).alias("sem_entregador"),
                                  F.sum(F.col("delivered_at").isNull().cast("int")).alias("sem_entrega")))
display(ped.select("customer.city").groupBy("city").count().orderBy("city"))
display(ped.selectExpr("customer.email LIKE '%quickbite.test' AS conta_teste", "customer.id").groupBy("conta_teste").agg(F.countDistinct("id").alias("clientes"), F.count("*").alias("linhas")))
display(ped.select(F.explode("items").alias("i")).select(F.regexp_replace("i.price", "[0-9]", "9").alias("formato")).groupBy("formato").count().orderBy(F.desc("count")))

# COMMAND ----------

rat = spark.read.option("header", True).csv(f"{ORIGEM}/ratings.csv")
display(rat.groupBy("stars").count().orderBy("stars"))
display(rat.join(ped.select("order_id").distinct(), "order_id", "left_anti").agg(F.count("*").alias("avaliacoes_de_pedidos_desconhecidos"), F.min("order_id"), F.max("order_id")))
display(spark.read.option("header", True).csv(f"{ORIGEM}/restaurants.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Catálogo de problemas (referência do instrutor)**
# MAGIC
# MAGIC | # | Problema | Onde | Quanto | Decisão |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | Pedidos **repetidos** em arquivos diferentes (reenvio idêntico) | pedidos | 786 linhas → 743 pedidos | manter 1 linha por `order_id` |
# MAGIC | 2 | **Contas de teste** (`@quickbite.test`) | pedidos | 34 pedidos | fora da análise, na quarentena |
# MAGIC | 3 | **Preço como texto** em 6 formatos (`€11.90`, `4,50 €`, `13.50 €`...) | itens | 996 itens fora do padrão `0.00 €` | converter para decimal em euros |
# MAGIC | 4 | **Cidade suja** (`' lisboa'`, `LISBOA`, `Coimbra `) | pedidos e restaurantes | muitas linhas | `trim` + capitalização |
# MAGIC | 5 | **Cancelados** sem entregador e sem hora de entrega | pedidos | 72 entre os válidos | esperado; fora do tempo de entrega e da receita |
# MAGIC | 6 | **Nota fora da faixa** (0 e 6) | avaliações | 19 | quarentena |
# MAGIC | 7 | **Avaliações de pedidos inexistentes** (chegarão em outro lote, ou nunca) | avaliações | 170 | quarentena, **reavaliada a cada execução** |
# MAGIC | 8 | Avaliações de pedidos de teste | avaliações | 13 | quarentena |
# MAGIC | 9 | Comentário vazio | avaliações | 110 | esperado; manter nulo |
# MAGIC | 10 | O nome do arquivo (`lunch`/`dinner`) **não** indica a refeição | pedidos | todos | derivar o período pela hora |
# MAGIC | 11 | A "cidade do cliente" muda a cada pedido (é a **cidade de entrega**) | pedidos | 114 de 118 clientes | atributo do pedido, não do cliente |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Landing e simulador de chegada
# MAGIC Os arquivos **não** vão direto para o pipeline: chegam num volume `landing`. Abra o notebook `apoio/01_simulador_landing`, configure os mesmos catálogo/schema e **copie o lote 1** (sem isso o pipeline não tem o que ler e falha). Depois volte aqui.
# MAGIC
# MAGIC **Pense:**
# MAGIC - Por que é útil separar *origem*, *landing* e *tabelas*?
# MAGIC - O gatilho de arquivo só dispara com arquivo **novo** (sobrescrever não conta). O que isso exige de quem entrega os arquivos?

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta de referência:** O landing é a fronteira: tudo que entra fica registrado e imutável (auditoria e reprocesso). O gatilho exige nomes únicos por entrega, por exemplo data/hora no nome do arquivo, e nunca sobrescrever um arquivo existente.

# COMMAND ----------

for p in ["orders", "ratings", "restaurants", "couriers"]:
    print(f"{p:<12}", sorted(os.path.basename(f) for f in glob.glob(f"{LANDING}/{p}/*")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. O pipeline: bronze → silver → gold
# MAGIC Um **pipeline declarativo** (Lakeflow Spark Declarative Pipelines). Vocês descrevem **o que** cada tabela contém; o Databricks resolve a ordem, o processamento incremental e as dependências.
# MAGIC
# MAGIC **Como criar:** *Jobs & Pipelines → Create → ETL pipeline*. Catálogo e schema de destino: os do setup. Em *Configuration*, defina os parâmetros que o código lê: `landing_path`, `sla_minutes`, `qb_catalog`, `qb_schema`. Fonte do código: uma pasta com arquivos `.sql` ou `.py`.
# MAGIC
# MAGIC **Regras do jogo**
# MAGIC - **Streaming tables** para o que **chega aos poucos** (ler só o que é novo). **Materialized views** para o que é **derivado** de outras tabelas.
# MAGIC - Leitura dos arquivos do landing: nada de listar arquivos manualmente, e **sem** `collect()`.
# MAGIC - Tudo reexecutável: rodar de novo não duplica nada.
# MAGIC - Código **declarativo**: sem `INSERT`, `MERGE` manual ou loops de carga.
# MAGIC
# MAGIC ### 3.1 Bronze
# MAGIC | Tabela | Fonte | Requisito |
# MAGIC |---|---|---|
# MAGIC | `bronze_orders`, `bronze_ratings`, `bronze_restaurants`, `bronze_couriers` | pastas do landing | Cópia **fiel**: nenhuma limpeza, CSVs sem inferir tipos (tudo texto). Cada linha guarda **de que arquivo veio** e **quando foi ingerida** |
# MAGIC
# MAGIC **Pense:** por que guardar "sujo" na bronze? O que muda se descobrirmos um erro na regra de limpeza daqui a um mês?
# MAGIC
# MAGIC ### 3.2 Silver
# MAGIC Princípios: **uma linha por entidade**, **tipos corretos**, **valores padronizados**, e **nada some em silêncio**.
# MAGIC
# MAGIC | Tabela | Grão | Conteúdo | Regras de negócio |
# MAGIC |---|---|---|---|
# MAGIC | `silver_orders` | pedido | id, cliente, e-mail, **cidade de entrega padronizada**, restaurante, entregador, status, `created_at`/`delivered_at` como timestamp, data e hora, **período da refeição**, **minutos de entrega**, nº de itens, quantidade total, **valor total em euros** | Cada pedido uma única vez, mesmo reenviado em outro arquivo. Contas de teste não entram. Minutos de entrega só para entregues. Valor calculado a partir dos itens |
# MAGIC | `silver_order_items` | item do pedido | pedido, posição, prato, quantidade, **preço unitário numérico**, total da linha | Preço sempre em euros, 2 casas |
# MAGIC | `silver_customers` | cliente | id, e-mail, 1º e último pedido, nº de pedidos | Derivada dos pedidos válidos |
# MAGIC | `silver_restaurants`, `silver_couriers` | cadastro | atributos padronizados | Se o cadastro chegar de novo, vale a última versão |
# MAGIC | `silver_ratings` | avaliação | pedido, nota inteira, comentário | Nota de 1 a 5. Uma avaliação por pedido |
# MAGIC | `quarantine_orders` | pedido rejeitado | colunas úteis + **motivo** | |
# MAGIC | `quarantine_ratings` | avaliação rejeitada | colunas úteis + **motivo específico** | Inclui avaliação de pedido ainda desconhecido, de conta de teste, de pedido cancelado |
# MAGIC | `silver_quality_report` | regra de qualidade | etapa, regra, ação (corrigiu, descartou, quarentena, informativo), nº de linhas | Tem de responder "quanto lixo havia?" com uma consulta |
# MAGIC
# MAGIC **Requisitos técnicos**
# MAGIC - Use **expectations** para as regras de qualidade. Decidam, regra a regra, se o comportamento é **avisar**, **descartar a linha** ou **falhar o pipeline**, e justifiquem.
# MAGIC - A **deduplicação** de pedidos entre arquivos tem de funcionar no modo incremental (pensem em `AUTO CDC`).
# MAGIC
# MAGIC **Pense antes de codar**
# MAGIC 1. Quando o mesmo `order_id` aparece em dois arquivos, qual linha vale? E se as linhas fossem diferentes?
# MAGIC 2. Avaliação chegou **antes** do pedido: descarta, guarda, ou espera? Como o pipeline "percebe" quando o pedido chega?
# MAGIC 3. Por que a quarentena de avaliações é melhor como *materialized view* do que como *streaming table*?
# MAGIC 4. Cancelado tem tempo de entrega nulo. O que acontece com a média se colocarmos zero?

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta de referência:** (1) O mais recente (por arquivo/ingestão); com linhas idênticas tanto faz, mas definir o critério protege contra reenvio com correção. (2) Fica de fora do silver_ratings válido, entra na quarentena com motivo `unknown_order`. Como a quarentena e a gold são *materialized views*, o pipeline reavalia tudo a cada atualização e a avaliação passa a valer sozinha quando o pedido chega. (3) Uma streaming table só processa o que é novo: uma avaliação já processada nunca seria reavaliada. A MV recalcula. (4) Zero puxaria a média para baixo; nulo é ignorado pela média.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Gold
# MAGIC Camada de **consumo**: nomes claros, regras de negócio explícitas, desempenho. Modelo **estrela** + tabelas de KPI.
# MAGIC
# MAGIC | Tabela | Grão | Conteúdo mínimo |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | dia | período dos pedidos: ano, mês, dia, **dia da semana (número e nome)**, semana, fim de semana? |
# MAGIC | `dim_restaurant`, `dim_courier`, `dim_customer` | entidade | atributos |
# MAGIC | `dim_dish` | prato | nome, culinária, preço de tabela |
# MAGIC | `fact_orders` | pedido | chaves das dimensões, status, valores, tempos, **flags** (`is_delivered`, `is_cancelled`, `within_sla`), **receita**, **receita perdida**, nota e comentário |
# MAGIC | `fact_order_items` | item | chave do pedido, prato, quantidade, preço, total |
# MAGIC | `gold_daily_kpis` | dia × cidade | alimenta a visão geral do app |
# MAGIC | `gold_restaurant_summary`, `gold_courier_summary`, `gold_dish_ranking`, `gold_hourly_demand`, `gold_rating_reasons` | conforme o nome | rankings e mapa de calor do app |
# MAGIC
# MAGIC **Definições de negócio (implementar exatamente assim)**
# MAGIC - **Receita** = valor dos pedidos **entregues**. **Receita perdida** = valor dos **cancelados**.
# MAGIC - **Dentro do SLA** = entregue com tempo **≤ SLA** (parâmetro `sla_minutes`). Para não entregues o valor é **nulo**, não falso.
# MAGIC - **Ticket médio** = receita ÷ pedidos entregues. **Taxa de cancelamento** = cancelados ÷ pedidos totais.
# MAGIC
# MAGIC **Relacionamentos:** declarem **chaves primárias** (coluna `NOT NULL`) e **estrangeiras** do fato para as dimensões, e comentem tabelas e colunas-chave. No Unity Catalog essas chaves são **informativas**: não impedem linha inválida. Então para que servem? E o que garante que não há órfãos?
# MAGIC
# MAGIC **Cuidado em `gold_daily_kpis`:** o app soma vários dias e cidades. **Uma média diária pode ser somada ou re-promediada?** O que guardar, então, para o app calcular ticket médio, tempo médio e nota média de qualquer recorte?

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta de referência:** As chaves servem para documentar o modelo (BI e Catalog Explorer desenham o diagrama), orientar o otimizador e descrever o contrato. Quem garante a integridade são os testes e as regras de carga. Em `gold_daily_kpis` guardam-se numeradores e denominadores (soma de minutos, nº de entregas, soma de estrelas, nº de avaliações): o app divide no final, e o resultado vale para qualquer recorte.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Código do pipeline (solução)
# MAGIC Os arquivos estão no repositório, na pasta `solucao/pipeline_sql` e na `solucao/pipeline_python`, com o **mesmo resultado**. Leia-os em aula; a célula seguinte cria o pipeline pelo SDK.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines, jobs

w = WorkspaceClient()
PASTA_PIPELINE = f"/Workspace{RAIZ_REPO}/solucao/pipeline_{LINGUAGEM}"
EXT = ".sql" if LINGUAGEM == "sql" else ".py"
arquivos = sorted(f"{PASTA_PIPELINE}/{f}" for f in os.listdir(PASTA_PIPELINE) if f.endswith(EXT))
print(*arquivos, sep="\n")

NOME_PIPELINE = f"quickbite_pipeline_{LINGUAGEM}"
config = {"landing_path": LANDING, "sla_minutes": str(SLA_MIN), "qb_catalog": catalogo, "qb_schema": schema}
existente = next((p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{NOME_PIPELINE}'")), None)
if existente:
    PIPELINE_ID = existente.pipeline_id
    print("pipeline já existe:", PIPELINE_ID)
else:
    criado = w.pipelines.create(
        name=NOME_PIPELINE, catalog=catalogo, schema=schema, serverless=True, continuous=False, configuration=config,
        libraries=[pipelines.PipelineLibrary(file=pipelines.FileLibrary(path=p)) for p in arquivos])
    PIPELINE_ID = criado.pipeline_id
    print("pipeline criado:", PIPELINE_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primeira execução manual
# MAGIC Com o lote 1 no landing, rode o pipeline **uma vez à mão** para ver o grafo e os resultados das expectations.

# COMMAND ----------

assert glob.glob(f"{LANDING}/orders/*.json"), "Landing vazio: rode o notebook apoio/01_simulador_landing (lote 1) antes de executar o pipeline."
upd = w.pipelines.start_update(pipeline_id=PIPELINE_ID)
w.pipelines.wait_get_pipeline_idle(PIPELINE_ID)
print("update:", upd.update_id, "| estado:", w.pipelines.get_update(PIPELINE_ID, upd.update_id).update.state)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Conferir os resultados
# MAGIC Valide os números **antes** de ir para o app: um painel bonito com número errado é pior que nenhum. Compare **dois caminhos** para o mesmo KPI (por exemplo receita direto do fato e somando o mart) e confirme: chaves únicas, nenhum órfão, cancelados sem tempo de entrega, entradas = saídas + quarentena + duplicados.

# COMMAND ----------

def n(q): return spark.sql(q).first()[0]
def conferir(nome, ok): print(("✅ " if ok else "❌ ") + nome)
T = f"`{catalogo}`.`{schema}`"

conferir("fact_orders: 1 linha por order_id", n(f"SELECT count(*) FROM {T}.fact_orders") == n(f"SELECT count(DISTINCT order_id) FROM {T}.fact_orders"))
conferir("fato sem restaurante órfão", n(f"SELECT count(*) FROM {T}.fact_orders f LEFT ANTI JOIN {T}.dim_restaurant d USING (restaurant_id)") == 0)
conferir("fato sem entregador órfão (ignora nulos)", n(f"SELECT count(*) FROM {T}.fact_orders f LEFT ANTI JOIN {T}.dim_courier d USING (courier_id) WHERE f.courier_id IS NOT NULL") == 0)
conferir("itens sem pedido/prato órfão", n(f"SELECT count(*) FROM {T}.fact_order_items i LEFT ANTI JOIN {T}.fact_orders f USING (order_id)") + n(f"SELECT count(*) FROM {T}.fact_order_items i LEFT ANTI JOIN {T}.dim_dish d USING (dish)") == 0)
conferir("receita: fato = mart = itens", abs(n(f"SELECT sum(revenue) FROM {T}.fact_orders") - n(f"SELECT sum(revenue) FROM {T}.gold_daily_kpis")) < 0.01
         and abs(n(f"SELECT sum(revenue) FROM {T}.fact_orders") - n(f"SELECT sum(line_total) FROM {T}.fact_order_items WHERE status = 'delivered'")) < 0.01)
conferir("cancelados sem tempo de entrega", n(f"SELECT count(*) FROM {T}.fact_orders WHERE is_cancelled AND delivery_minutes IS NOT NULL") == 0)
conferir("within_sla nulo fora das entregas", n(f"SELECT count(*) FROM {T}.fact_orders WHERE NOT is_delivered AND within_sla IS NOT NULL") == 0)
display(spark.sql(f"SELECT * FROM {T}.silver_quality_report ORDER BY step"))
display(spark.sql(f"""SELECT round(sum(revenue), 2) AS receita, round(sum(lost_revenue), 2) AS perdida, count(*) AS pedidos,
  round(100 * sum(CASE WHEN is_cancelled THEN 1 END) / count(*), 1) AS cancel_pct, round(avg(delivery_minutes), 1) AS tempo_medio,
  round(100 * avg(CASE WHEN within_sla THEN 1.0 ELSE 0.0 END), 1) AS pct_sla, round(avg(stars), 2) AS nota_media FROM {T}.fact_orders"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Números esperados com os 8 lotes no landing** (para o instrutor conferir):
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | `bronze_orders` | 786 |
# MAGIC | `silver_orders` | 709 (43 duplicados e 34 de teste ficam de fora) |
# MAGIC | `silver_order_items` | 1.394 |
# MAGIC | `quarantine_orders` | 34 |
# MAGIC | avaliações ligadas a pedido entregue | 385 |
# MAGIC | `quarantine_ratings` | 202 (170 pedido desconhecido, 19 nota fora da faixa, 13 conta de teste) |
# MAGIC | receita / perdida | € 20.584,50 / € 2.380,10 |
# MAGIC | cancelamento · tempo médio · % no SLA (45 min) · nota média | 10,2% · 37,4 min · 61,8% · 3,71 |
# MAGIC
# MAGIC Com **menos lotes** os números são menores. Em particular, a quarentena de avaliações começa grande e **encolhe** à medida que os pedidos chegam.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Databricks App
# MAGIC Um **Databricks App** roda uma aplicação web (Streamlit, Dash, Gradio...) **dentro** do workspace, com autenticação e acesso aos dados governados pelo Unity Catalog. Vamos usar **Streamlit**.
# MAGIC
# MAGIC ### 4.1 Requisitos do app
# MAGIC **Filtros (barra lateral):** cidade de entrega (múltipla escolha) e período.
# MAGIC
# MAGIC | Aba | Deve mostrar |
# MAGIC |---|---|
# MAGIC | Visão geral | Pedidos, receita, ticket médio, taxa de cancelamento, tempo médio, % no SLA, nota média, receita perdida. Receita por dia. Pedidos e cancelamentos por dia. Quadro por cidade |
# MAGIC | Restaurantes | Receita por restaurante (filtro por culinária); tabela com cancelamento, tempo e nota |
# MAGIC | Entregadores | % de entregas no prazo por entregador; comparação por veículo |
# MAGIC | Pratos | Pratos mais vendidos (unidades e receita) |
# MAGIC | Horários | Mapa de calor da demanda: dia da semana × hora |
# MAGIC | Avaliações | Distribuição das notas, comentários mais frequentes, cobertura das avaliações |
# MAGIC | Qualidade dos dados | O relatório de qualidade da silver |
# MAGIC
# MAGIC **Regras:** o app lê **somente a gold** (e o relatório de qualidade); os KPIs **batem** com as conferências da seção 3.4; nada de ID de warehouse ou token no código (use recursos e variáveis de ambiente); consultas com cache.
# MAGIC
# MAGIC ### 4.2 O que o app precisa
# MAGIC Uma pasta com `app.py` (código), `app.yaml` (como iniciar e variáveis de ambiente) e `requirements.txt` (pacotes extras).
# MAGIC
# MAGIC `app.yaml` (modelo):
# MAGIC ```yaml
# MAGIC command: ['streamlit', 'run', 'app.py']
# MAGIC env:
# MAGIC   - name: 'DATABRICKS_WAREHOUSE_ID'
# MAGIC     valueFrom: 'sql-warehouse'      # chave do recurso "SQL warehouse" adicionado ao app
# MAGIC   - name: 'QB_CATALOG'
# MAGIC     value: 'workspace'
# MAGIC   - name: 'QB_SCHEMA'
# MAGIC     value: 'quickbite'
# MAGIC ```
# MAGIC Conexão (boilerplate, o que importa é o que vocês fazem com ela):
# MAGIC ```python
# MAGIC import os
# MAGIC from databricks import sql
# MAGIC from databricks.sdk.core import Config
# MAGIC
# MAGIC cfg = Config()   # credenciais do service principal do app
# MAGIC conn = sql.connect(server_hostname=cfg.host,
# MAGIC                    http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
# MAGIC                    credentials_provider=lambda: cfg.authenticate)
# MAGIC ```
# MAGIC
# MAGIC ### 4.3 Permissões (a armadilha clássica)
# MAGIC Cada app roda com um **service principal próprio**, que **não é você**. Ele precisa de **CAN USE** no SQL warehouse (adicionado como *recurso* do app) e de **USE CATALOG, USE SCHEMA e SELECT** nas tabelas gold.
# MAGIC
# MAGIC **Pense:** o app abre e mostra "tabela não encontrada" mesmo com a tabela existindo. Qual das duas permissões você verifica primeiro?

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta de referência:** As permissões do Unity Catalog do service principal do app (USE CATALOG, USE SCHEMA, SELECT): o warehouse já abriu a conexão, e o erro vem da consulta. O app não herda as suas permissões.

# COMMAND ----------

# MAGIC %md
# MAGIC O app da solução está em `solucao/app/` (`app.py`, `app.yaml`, `requirements.txt`). A célula cria o app, adiciona o SQL warehouse como recurso e concede as permissões ao service principal. Se algo falhar, ela mostra os passos manuais (*Compute → Apps → Create app*).

# COMMAND ----------

from databricks.sdk.service import apps as ap
NOME_APP = "quickbite-dashboard"
SP = None
try:
    wh = next((x for x in w.warehouses.list()), None)
    assert wh is not None, "nenhum SQL warehouse encontrado"
    try:
        app = w.apps.get(NOME_APP)
        print("app já existe")
    except Exception:
        app = w.apps.create_and_wait(app=ap.App(
            name=NOME_APP, description="Painel QuickBite",
            resources=[ap.AppResource(name="sql-warehouse", sql_warehouse=ap.AppResourceSqlWarehouse(
                id=wh.id, permission=ap.AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE))]))
        print("app criado:", app.name)
    SP = app.service_principal_client_id
    print("service principal:", SP)
except Exception as e:
    print("Não consegui criar o app automaticamente:", str(e)[:200])
    print("Crie-o em Compute > Apps > Create app, com o recurso SQL warehouse (chave sql-warehouse, Can use).")

# COMMAND ----------

if SP:
    for stmt in [f"GRANT USE CATALOG ON CATALOG `{catalogo}` TO `{SP}`",
                 f"GRANT USE SCHEMA ON SCHEMA `{catalogo}`.`{schema}` TO `{SP}`",
                 f"GRANT SELECT ON SCHEMA `{catalogo}`.`{schema}` TO `{SP}`"]:
        try:
            spark.sql(stmt); print("ok  ", stmt)
        except Exception as e:
            print("falha", stmt, "->", str(e)[:120])
else:
    print("Defina SP com o service principal client id do app e rode os três GRANTs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. O job: gatilho de arquivo → pipeline → atualizar o app
# MAGIC Agora automatizamos tudo. Um **job** com:
# MAGIC
# MAGIC | Item | Requisito |
# MAGIC |---|---|
# MAGIC | Gatilho | **Chegada de arquivo** no volume landing. Espere após a última mudança (um lote são vários arquivos): o valor tem de ser **maior que 60 segundos**. Não dispare mais de uma vez por minuto |
# MAGIC | Tarefa 1 | Atualização do **pipeline** (incremental, **sem** full refresh) |
# MAGIC | Tarefa 2 | **Só se a 1 der certo**: validar que a gold tem dados e reimplantar o app. Use o notebook `apoio/02_deploy_app` |
# MAGIC | Concorrência | Uma execução por vez |
# MAGIC
# MAGIC **Pense:**
# MAGIC 1. Dois lotes chegam em sequência rápida. O que acontece se o job ainda estiver rodando? Por que limitar a uma execução?
# MAGIC 2. O app lê a gold por SQL a cada consulta. Então por que reimplantar o app? (Há uma resposta honesta: nem sempre é necessário. Quando é?)
# MAGIC 3. A tarefa 1 falha por uma expectation `FAIL UPDATE`. A tarefa 2 roda? O que o usuário do painel vê?

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta de referência:** (1) Com uma execução por vez, a nova chegada espera (ou é agrupada); duas atualizações simultâneas do mesmo pipeline não são possíveis e só gerariam erro. (2) Para dados novos não é necessário: o app consulta a gold a cada acesso (com cache de alguns minutos). Reimplantar é preciso quando o **código** do app mudou; aqui ele serve para praticar a orquestração e limpar o cache. (3) Não roda (dependência), e o painel continua mostrando a última versão válida da gold.

# COMMAND ----------

NOTEBOOK_TAREFA2 = f"{RAIZ_REPO}/apoio/02_deploy_app"   # caminho no workspace, sem /Workspace
params = {"catalogo": catalogo, "schema": schema, "app_name": NOME_APP}

job = w.jobs.create(
    name="quickbite_etl",
    max_concurrent_runs=1,
    trigger=jobs.TriggerSettings(
        pause_status=jobs.PauseStatus.UNPAUSED,
        file_arrival=jobs.FileArrivalTriggerConfiguration(
            url=LANDING + "/", min_time_between_triggers_seconds=60, wait_after_last_change_seconds=61)),
    tasks=[
        jobs.Task(task_key="pipeline", pipeline_task=jobs.PipelineTask(pipeline_id=PIPELINE_ID, full_refresh=False)),
        jobs.Task(task_key="atualizar_app", depends_on=[jobs.TaskDependency(task_key="pipeline")],
                  notebook_task=jobs.NotebookTask(notebook_path=NOTEBOOK_TAREFA2, base_parameters=params)),
    ])
print("job criado:", job.job_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstração
# MAGIC Abra o notebook `apoio/01_simulador_landing`, copie o **lote 2**, e volte aqui para acompanhar as execuções. Em seguida copie os demais lotes.

# COMMAND ----------

import time
time.sleep(150)   # o gatilho espera mais de 60 s depois da última mudança, mais o tempo de subir o job
for r in w.jobs.list_runs(job_id=job.job_id, limit=5):
    print(r.run_id, r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "?",
          r.state.result_state.value if r.state and r.state.result_state else "-")

# COMMAND ----------

# a quarentena de avaliações encolhe conforme os pedidos chegam
display(spark.sql(f"SELECT reason, count(*) AS n FROM `{catalogo}`.`{schema}`.quarantine_ratings GROUP BY reason ORDER BY n DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Para fechar
# MAGIC 1. Qual problema de qualidade mais **distorceria** um KPI se não fosse tratado? Qual KPI?
# MAGIC 2. Por que o app lê só a gold? O que acontece se um analista passar a consultar a bronze?
# MAGIC 3. O app mostra uma receita diferente da conferência da seção 3.4. Por onde você começa a investigar?
# MAGIC 4. O que muda se, em vez de um lote por vez, os arquivos passarem a chegar a cada minuto?

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta de referência:** (1) Duplicados e contas de teste inflariam pedidos, receita e ticket; cancelados somados como receita também. (2) A gold é o contrato: regras de negócio já aplicadas, nomes claros, desempenho. Na bronze cada analista reinventaria a limpeza. (3) Filtro do app versus o da conferência, depois a definição (entregue ou não), depois o cache. (4) Trocar o gatilho de arquivo por execução contínua (ou agendada, de minuto em minuto) e verificar custo, já que o pipeline fica ligado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Desafios extras
# MAGIC 1. **SCD tipo 2** em `dim_restaurant` (cidade e culinária mudam) com o fato apontando para a versão vigente no dia do pedido.
# MAGIC 2. **Alerta** no Databricks SQL quando o cancelamento diário passar de um limite.
# MAGIC 3. **Genie:** um espaço Genie sobre as tabelas gold; os comentários ajudaram?
# MAGIC 4. **Expectations no log de eventos:** acompanhem as métricas de qualidade do pipeline pelo `event_log()`.
# MAGIC 5. **Aba de quarentena** no app, com o motivo de cada rejeição.
