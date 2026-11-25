# Databricks notebook source

# MAGIC %md
# MAGIC # Apoio · Simulador de chegada de arquivos
# MAGIC Copia **um lote por vez** do volume de origem (`quickbite_files`) para o volume `landing`, como se os arquivos chegassem do mundo real. Cada cópia nova dispara o job (se o gatilho estiver ativo).
# MAGIC
# MAGIC **Como os lotes são montados**
# MAGIC | Lote | Conteúdo |
# MAGIC |---|---|
# MAGIC | 1 | pedidos do 1º dia + `restaurants.csv` + `couriers.csv` |
# MAGIC | 2 a 7 | pedidos do dia + avaliações dos pedidos do **dia anterior** (a avaliação chega depois do pedido) |
# MAGIC | 2 | também recebe as avaliações de pedidos que **ainda não existem** (chegarão em outro lote, ou nunca) |
# MAGIC | 8 | só as avaliações do último dia |
# MAGIC
# MAGIC Pedidos reenviados em arquivos diferentes (duplicados) são copiados como estão: o pipeline é que tem de lidar com eles.

# COMMAND ----------

import os, re, glob, shutil, json
import pandas as pd
from datetime import date, timedelta

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema", "quickbite", "Schema")
dbutils.widgets.text("volume_origem", "quickbite_files", "Volume de origem")
dbutils.widgets.text("volume_landing", "landing", "Volume de landing")
dbutils.widgets.text("n_lotes", "1", "Quantos lotes copiar agora")
dbutils.widgets.dropdown("limpar_landing", "nao", ["nao", "sim"], "Esvaziar o landing antes?")

catalogo, schema = dbutils.widgets.get("catalogo").strip(), dbutils.widgets.get("schema").strip()
ORIGEM = f"/Volumes/{catalogo}/{schema}/{dbutils.widgets.get('volume_origem').strip()}"
VOL_LANDING = dbutils.widgets.get("volume_landing").strip()
LANDING = f"/Volumes/{catalogo}/{schema}/{VOL_LANDING}"

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalogo}`.`{schema}`.`{VOL_LANDING}`")
PASTAS = ["orders", "ratings", "restaurants", "couriers"]
for p in PASTAS:
    os.makedirs(f"{LANDING}/{p}", exist_ok=True)
print("origem :", ORIGEM)
print("landing:", LANDING)

# COMMAND ----------

if dbutils.widgets.get("limpar_landing") == "sim":
    for p in PASTAS:
        for f in glob.glob(f"{LANDING}/{p}/*"):
            os.remove(f)
    print("Landing esvaziado. Se o pipeline já rodou, faça um full refresh dele.")

# COMMAND ----------

def ler_jsonl(caminho):
    with open(caminho, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]

def arquivos_de_pedidos():
    achados = {}
    for f in sorted(glob.glob(f"{ORIGEM}/orders_*.json")):
        d = re.search(r"orders_(\d{4}-\d{2}-\d{2})_", f).group(1)
        achados.setdefault(d, []).append(f)
    return achados

PEDIDOS = arquivos_de_pedidos()
DIAS = sorted(PEDIDOS)
assert DIAS, f"Nenhum orders_*.json em {ORIGEM}"

# dia de cada pedido (pelo created_at) e avaliações lidas da origem
DIA_DO_PEDIDO = {}
for d, fs in PEDIDOS.items():
    for f in fs:
        for o in ler_jsonl(f):
            DIA_DO_PEDIDO[o["order_id"]] = d
AVALIACOES = pd.read_csv(f"{ORIGEM}/ratings.csv", dtype=str, keep_default_na=False)

def rotulo(k):
    """Data que nomeia o lote k (o último lote não tem pedidos e usa o dia seguinte)."""
    return DIAS[k] if k < len(DIAS) else (date.fromisoformat(DIAS[-1]) + timedelta(days=1)).isoformat()

N_LOTES = len(DIAS) + 1

def lote_feito(k):
    return os.path.exists(f"{LANDING}/ratings/ratings_{rotulo(k)}.csv")

def avaliacoes_do_lote(k):
    ids_dia_anterior = {o for o, d in DIA_DO_PEDIDO.items() if k >= 1 and d == DIAS[k - 1]}
    sel = AVALIACOES["order_id"].isin(ids_dia_anterior)
    if k == 1:
        sel = sel | ~AVALIACOES["order_id"].isin(DIA_DO_PEDIDO.keys())
    return AVALIACOES[sel]

def copiar_lote(k):
    r = rotulo(k)
    if k < len(DIAS):
        for f in PEDIDOS[DIAS[k]]:
            shutil.copy(f, f"{LANDING}/orders/{os.path.basename(f)}")
    if k == 0:
        shutil.copy(f"{ORIGEM}/restaurants.csv", f"{LANDING}/restaurants/restaurants.csv")
        shutil.copy(f"{ORIGEM}/couriers.csv", f"{LANDING}/couriers/couriers.csv")
    # as avaliações são gravadas por último: o arquivo funciona como marca de "lote concluído"
    avaliacoes_do_lote(k).to_csv(f"{LANDING}/ratings/ratings_{r}.csv", index=False)
    print(f"lote {k + 1}/{N_LOTES} copiado ({r}): {len(PEDIDOS.get(r, []))} arquivo(s) de pedidos, {len(avaliacoes_do_lote(k))} avaliações")

def proximo_lote(n=1):
    feitos = 0
    for k in range(N_LOTES):
        if feitos >= n:
            break
        if not lote_feito(k):
            copiar_lote(k); feitos += 1
    if feitos == 0:
        print("Todos os lotes já foram copiados.")

print("lotes já no landing:", [k + 1 for k in range(N_LOTES) if lote_feito(k)] or "nenhum")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copiar o(s) próximo(s) lote(s)
# MAGIC Ajuste o widget **Quantos lotes copiar agora** e rode a célula. Em aula, copie **um lote**, espere o job disparar e mostre o pipeline processando.

# COMMAND ----------

proximo_lote(int(dbutils.widgets.get("n_lotes")))

# COMMAND ----------

# onde estamos
for p in PASTAS:
    print(f"{p:<12}", sorted(os.path.basename(f) for f in glob.glob(f"{LANDING}/{p}/*")))
