# Databricks notebook source

# MAGIC %md
# MAGIC # Projeto final · QuickBite (EXERCÍCIO)
# MAGIC **Do arquivo que chega ao app: landing → pipeline (bronze, silver, gold) → job com gatilho → Databricks App**
# MAGIC
# MAGIC > Aqui **não há receitas**. Cada etapa traz os requisitos de negócio e o que precisa existir ao final. **Como chegar lá é com vocês.**
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

# exploração

# COMMAND ----------

# exploração

# COMMAND ----------

# exploração

# COMMAND ----------

# exploração

# COMMAND ----------

# MAGIC %md
# MAGIC > ✍️ **Meu catálogo de problemas** *(problema · onde · quantas linhas · o que decidi)*
# MAGIC >
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Landing e simulador de chegada
# MAGIC Os arquivos **não** vão direto para o pipeline: chegam num volume `landing`. Abra o notebook `apoio/01_simulador_landing`, configure os mesmos catálogo/schema e **copie o lote 1**. Depois volte aqui.
# MAGIC
# MAGIC **Pense:**
# MAGIC - Por que é útil separar *origem*, *landing* e *tabelas*?
# MAGIC - O gatilho de arquivo só dispara com arquivo **novo** (sobrescrever não conta). O que isso exige de quem entrega os arquivos?

# COMMAND ----------

# MAGIC %md
# MAGIC > ✍️ **Sua resposta:** *(clique duas vezes e escreva)*
# MAGIC >
# MAGIC > 

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
# MAGIC > ✍️ **Sua resposta:** *(clique duas vezes e escreva)*
# MAGIC >
# MAGIC > 

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
# MAGIC > ✍️ **Sua resposta:** *(clique duas vezes e escreva)*
# MAGIC >
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mãos à obra
# MAGIC Crie os arquivos do pipeline em `exercicio/pipeline/` (já existem arquivos vazios com cabeçalho), crie o pipeline na interface e rode **uma vez** com o lote 1 no landing.

# COMMAND ----------

# opcional: anote aqui o id do pipeline e comandos de apoio

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Conferir os resultados
# MAGIC Valide os números **antes** de ir para o app: um painel bonito com número errado é pior que nenhum. Compare **dois caminhos** para o mesmo KPI (por exemplo receita direto do fato e somando o mart) e confirme: chaves únicas, nenhum órfão, cancelados sem tempo de entrega, entradas = saídas + quarentena + duplicados.

# COMMAND ----------

# conferências

# COMMAND ----------

# conferências

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
# MAGIC > ✍️ **Sua resposta:** *(clique duas vezes e escreva)*
# MAGIC >
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mãos à obra
# MAGIC Escrevam o app numa pasta própria do workspace (por exemplo `exercicio/meu_app/`). Criem o app em *Compute → Apps*, adicionem o SQL warehouse como recurso, façam o deploy da pasta e concedam as permissões ao service principal.

# COMMAND ----------

# app.py

# COMMAND ----------

# app.yaml / requirements.txt

# COMMAND ----------

# GRANTs para o service principal do app

# COMMAND ----------

# MAGIC %md
# MAGIC > ✍️ **Cole aqui a URL do seu app.**
# MAGIC >
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. O job: gatilho de arquivo → pipeline → atualizar o app
# MAGIC Agora automatizamos tudo. Um **job** com:
# MAGIC
# MAGIC | Item | Requisito |
# MAGIC |---|---|
# MAGIC | Gatilho | **Chegada de arquivo** no volume landing. Espere alguns segundos após a última mudança (um lote são vários arquivos) e não dispare mais de uma vez por minuto |
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
# MAGIC > ✍️ **Sua resposta:** *(clique duas vezes e escreva)*
# MAGIC >
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mãos à obra
# MAGIC Crie o job (pela interface ou pelo SDK), copie os lotes seguintes com o simulador e **mostre** o job disparando sozinho. Anotem o que observaram.

# COMMAND ----------

# job: criação (UI ou SDK)

# COMMAND ----------

# acompanhar as execuções

# COMMAND ----------

# MAGIC %md
# MAGIC > ✍️ **O que aconteceu quando o lote 2 chegou? E quando chegaram dois lotes seguidos?**
# MAGIC >
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Para fechar
# MAGIC 1. Qual problema de qualidade mais **distorceria** um KPI se não fosse tratado? Qual KPI?
# MAGIC 2. Por que o app lê só a gold? O que acontece se um analista passar a consultar a bronze?
# MAGIC 3. O app mostra uma receita diferente da conferência da seção 3.4. Por onde você começa a investigar?
# MAGIC 4. O que muda se, em vez de um lote por vez, os arquivos passarem a chegar a cada minuto?

# COMMAND ----------

# MAGIC %md
# MAGIC > ✍️ **Sua resposta:** *(clique duas vezes e escreva)*
# MAGIC >
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Desafios extras
# MAGIC 1. **SCD tipo 2** em `dim_restaurant` (cidade e culinária mudam) com o fato apontando para a versão vigente no dia do pedido.
# MAGIC 2. **Alerta** no Databricks SQL quando o cancelamento diário passar de um limite.
# MAGIC 3. **Genie:** um espaço Genie sobre as tabelas gold; os comentários ajudaram?
# MAGIC 4. **Expectations no log de eventos:** acompanhem as métricas de qualidade do pipeline pelo `event_log()`.
# MAGIC 5. **Aba de quarentena** no app, com o motivo de cada rejeição.
