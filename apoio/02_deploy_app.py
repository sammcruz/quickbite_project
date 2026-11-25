# Databricks notebook source

# MAGIC %md
# MAGIC # Apoio · Tarefa 2 do job: validar a gold e atualizar o app
# MAGIC 1. Confere que a gold tem dados (se não tiver, a tarefa **falha** e o app não é tocado).
# MAGIC 2. Reimplanta o Databricks App (deploy da pasta do app) e inicia se estiver parado.
# MAGIC
# MAGIC Parâmetros recebidos do job: `catalogo`, `schema`, `app_name`, `app_path`. Se `app_path` ficar vazio, usa a pasta `solucao/app` do repositório.

# COMMAND ----------

import os
dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema", "quickbite", "Schema")
dbutils.widgets.text("app_name", "quickbite-dashboard", "Nome do app")
dbutils.widgets.text("app_path", "", "Pasta do app no workspace (vazio = solucao/app deste repositório)")

catalogo, schema = dbutils.widgets.get("catalogo"), dbutils.widgets.get("schema")
APP_NAME = dbutils.widgets.get("app_name").strip()
nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
raiz_repo = os.path.dirname(os.path.dirname(nb))
APP_PATH = dbutils.widgets.get("app_path").strip() or f"/Workspace{raiz_repo}/solucao/app"
print("app:", APP_NAME, "| pasta:", APP_PATH)

# COMMAND ----------

# 1) a gold precisa ter dados
n = spark.sql(f"SELECT count(*) FROM `{catalogo}`.`{schema}`.fact_orders").first()[0]
assert n > 0, "fact_orders está vazia: não vou atualizar o app"
print(f"fact_orders: {n:,} pedidos")

# COMMAND ----------

# 2) reimplantar o app
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode

w = WorkspaceClient()
app = w.apps.get(APP_NAME)
estado = app.compute_status.state.value if app.compute_status and app.compute_status.state else "?"
print("estado do compute:", estado)
if estado not in ("ACTIVE", "STARTING"):
    w.apps.start_and_wait(APP_NAME)

dep = w.apps.deploy_and_wait(app_name=APP_NAME, app_deployment=AppDeployment(source_code_path=APP_PATH, mode=AppDeploymentMode.SNAPSHOT))
print("deploy:", dep.status.state.value if dep.status and dep.status.state else dep)
print("URL:", w.apps.get(APP_NAME).url)
