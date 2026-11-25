import os
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

try:
    import altair as alt
except Exception:
    alt = None

CATALOG = os.getenv("QB_CATALOG", "workspace")
SCHEMA = os.getenv("QB_SCHEMA", "quickbite")
SLA_MIN = int(os.getenv("QB_SLA_MIN", "45"))
T = f"`{CATALOG}`.`{SCHEMA}`"

st.set_page_config(page_title="QuickBite", page_icon="🍔", layout="wide")


@st.cache_resource
def conexao():
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=300)
def consulta(q: str) -> pd.DataFrame:
    with conexao().cursor() as c:
        c.execute(q)
        return c.fetchall_arrow().to_pandas()


# ------------------------------------------------------------------ filtros (sidebar)
st.sidebar.title("QuickBite")
cidades_disp = consulta(f"SELECT DISTINCT delivery_city FROM {T}.fact_orders ORDER BY 1")["delivery_city"].tolist()
limites = consulta(f"SELECT min(order_date) AS d0, max(order_date) AS d1 FROM {T}.fact_orders").iloc[0]
cidades = st.sidebar.multiselect("Cidade de entrega", cidades_disp, default=cidades_disp)
periodo = st.sidebar.date_input("Período", value=(limites["d0"], limites["d1"]))
st.sidebar.caption(f"SLA de entrega: {SLA_MIN} min")

if not cidades:
    st.warning("Escolha ao menos uma cidade.")
    st.stop()
if isinstance(periodo, (tuple, list)) and len(periodo) == 2:
    d0, d1 = periodo
else:
    d0 = d1 = periodo if not isinstance(periodo, (tuple, list)) else periodo[0]

lista_cidades = ", ".join("'" + c.replace("'", "''") + "'" for c in cidades)
FILTRO = f"order_date BETWEEN DATE'{d0}' AND DATE'{d1}' AND delivery_city IN ({lista_cidades})"
FILTRO_F = FILTRO.replace("order_date", "f.order_date").replace("delivery_city", "f.delivery_city")

st.title("🍔 QuickBite · operação de delivery")

abas = st.tabs(["Visão geral", "Restaurantes", "Entregadores", "Pratos", "Horários", "Avaliações", "Qualidade dos dados"])

# ------------------------------------------------------------------ visão geral
with abas[0]:
    k = consulta(f"""
        SELECT sum(n_orders) AS pedidos, sum(n_delivered) AS entregues, sum(n_cancelled) AS cancelados,
               sum(revenue) AS receita, sum(lost_revenue) AS receita_perdida,
               sum(delivery_minutes_sum) / nullif(sum(n_delivered), 0) AS tempo_medio,
               sum(n_within_sla) / nullif(sum(n_delivered), 0) AS pct_sla,
               sum(stars_sum) / nullif(sum(n_ratings), 0) AS nota_media
        FROM {T}.gold_daily_kpis WHERE {FILTRO}""").iloc[0]
    c = st.columns(4)
    c[0].metric("Pedidos", f"{int(k['pedidos'] or 0):,}")
    c[1].metric("Receita (entregues)", f"€ {float(k['receita'] or 0):,.2f}")
    c[2].metric("Ticket médio", f"€ {float(k['receita'] or 0) / max(float(k['entregues'] or 0), 1):,.2f}")
    c[3].metric("Cancelamento", f"{100 * float(k['cancelados'] or 0) / max(float(k['pedidos'] or 0), 1):.1f}%")
    c = st.columns(4)
    c[0].metric("Tempo médio de entrega", f"{float(k['tempo_medio'] or 0):.1f} min")
    c[1].metric(f"Entregas em até {SLA_MIN} min", f"{100 * float(k['pct_sla'] or 0):.1f}%")
    c[2].metric("Nota média", f"{float(k['nota_media'] or 0):.2f}")
    c[3].metric("Receita perdida (cancelados)", f"€ {float(k['receita_perdida'] or 0):,.2f}")

    serie = consulta(f"""
        SELECT order_date, sum(revenue) AS receita, sum(n_orders) AS pedidos, sum(n_cancelled) AS cancelados
        FROM {T}.gold_daily_kpis WHERE {FILTRO} GROUP BY order_date ORDER BY order_date""")
    esq, dir_ = st.columns(2)
    esq.subheader("Receita por dia")
    esq.line_chart(serie.set_index("order_date")["receita"])
    dir_.subheader("Pedidos e cancelamentos por dia")
    dir_.bar_chart(serie.set_index("order_date")[["pedidos", "cancelados"]])

    st.subheader("Por cidade")
    st.dataframe(consulta(f"""
        SELECT delivery_city AS cidade, sum(n_orders) AS pedidos, round(sum(revenue), 2) AS receita,
               round(100 * sum(n_cancelled) / nullif(sum(n_orders), 0), 1) AS cancelamento_pct,
               round(sum(delivery_minutes_sum) / nullif(sum(n_delivered), 0), 1) AS tempo_medio_min
        FROM {T}.gold_daily_kpis WHERE {FILTRO} GROUP BY delivery_city ORDER BY receita DESC"""), use_container_width=True)

# ------------------------------------------------------------------ restaurantes
with abas[1]:
    rest = consulta(f"""
        SELECT r.restaurant_name AS restaurante, r.city AS cidade, r.cuisine AS culinaria,
               count(*) AS pedidos, round(sum(f.revenue), 2) AS receita,
               round(100 * avg(CASE WHEN f.is_cancelled THEN 1.0 ELSE 0.0 END), 1) AS cancelamento_pct,
               round(avg(f.delivery_minutes), 1) AS tempo_medio_min,
               round(avg(f.stars), 2) AS nota_media
        FROM {T}.fact_orders f JOIN {T}.dim_restaurant r ON r.restaurant_id = f.restaurant_id
        WHERE {FILTRO_F} GROUP BY r.restaurant_name, r.city, r.cuisine ORDER BY receita DESC""")
    culinarias = sorted(rest["culinaria"].dropna().unique().tolist())
    escolha = st.multiselect("Culinária", culinarias, default=culinarias)
    rest = rest[rest["culinaria"].isin(escolha)]
    st.subheader("Receita por restaurante")
    st.bar_chart(rest.set_index("restaurante")["receita"])
    st.dataframe(rest, use_container_width=True)

# ------------------------------------------------------------------ entregadores
with abas[2]:
    ent = consulta(f"""
        SELECT c.courier_name AS entregador, c.vehicle AS veiculo, count(*) AS entregas,
               round(avg(f.delivery_minutes), 1) AS tempo_medio_min,
               round(100 * avg(CASE WHEN f.within_sla THEN 1.0 ELSE 0.0 END), 1) AS dentro_sla_pct,
               round(avg(f.stars), 2) AS nota_media
        FROM {T}.fact_orders f JOIN {T}.dim_courier c ON c.courier_id = f.courier_id
        WHERE {FILTRO_F} AND f.is_delivered GROUP BY c.courier_name, c.vehicle ORDER BY dentro_sla_pct DESC""")
    st.subheader("Entregas no prazo por entregador")
    st.bar_chart(ent.set_index("entregador")["dentro_sla_pct"])
    st.subheader("Por tipo de veículo")
    st.dataframe(ent.groupby("veiculo").agg(entregas=("entregas", "sum"), tempo_medio_min=("tempo_medio_min", "mean")).round(1), use_container_width=True)
    st.dataframe(ent, use_container_width=True)

# ------------------------------------------------------------------ pratos
with abas[3]:
    pratos = consulta(f"""
        SELECT d.dish AS prato, d.cuisine AS culinaria, sum(i.quantity) AS unidades, round(sum(i.line_total), 2) AS receita
        FROM {T}.fact_order_items i
        JOIN {T}.fact_orders f ON f.order_id = i.order_id
        JOIN {T}.dim_dish d ON d.dish = i.dish
        WHERE {FILTRO_F} AND f.is_delivered GROUP BY d.dish, d.cuisine ORDER BY unidades DESC""")
    st.subheader("Pratos mais vendidos (unidades)")
    st.bar_chart(pratos.set_index("prato")["unidades"])
    st.dataframe(pratos, use_container_width=True)

# ------------------------------------------------------------------ horários
with abas[4]:
    h = consulta(f"""
        SELECT dow_num, day_name, order_hour, count(*) AS pedidos
        FROM {T}.fact_orders f JOIN {T}.dim_date dd ON dd.date_key = f.order_date
        WHERE {FILTRO_F} GROUP BY dow_num, day_name, order_hour ORDER BY dow_num, order_hour""")
    st.subheader("Demanda por dia da semana e hora")
    if alt is not None and len(h):
        st.altair_chart(alt.Chart(h).mark_rect().encode(
            x=alt.X("order_hour:O", title="Hora"), y=alt.Y("day_name:N", sort=alt.EncodingSortField("dow_num"), title=""),
            color=alt.Color("pedidos:Q", scale=alt.Scale(scheme="oranges")), tooltip=["day_name", "order_hour", "pedidos"]),
            use_container_width=True)
    else:
        st.dataframe(h.pivot(index="day_name", columns="order_hour", values="pedidos").fillna(0), use_container_width=True)

# ------------------------------------------------------------------ avaliações
with abas[5]:
    notas = consulta(f"""
        SELECT stars, count(*) AS avaliacoes FROM {T}.fact_orders f
        WHERE {FILTRO_F} AND stars IS NOT NULL GROUP BY stars ORDER BY stars""")
    esq, dir_ = st.columns(2)
    esq.subheader("Distribuição das notas")
    esq.bar_chart(notas.set_index("stars")["avaliacoes"])
    motivos = consulta(f"""
        SELECT coalesce(comment, '(sem comentário)') AS comentario, count(*) AS avaliacoes, round(avg(stars), 2) AS nota_media
        FROM {T}.fact_orders f WHERE {FILTRO_F} AND stars IS NOT NULL GROUP BY 1 ORDER BY avaliacoes DESC""")
    dir_.subheader("Comentários mais frequentes")
    dir_.dataframe(motivos, use_container_width=True)
    cob = consulta(f"""
        SELECT round(100 * count(stars) / nullif(count(*), 0), 1) AS cobertura_pct
        FROM {T}.fact_orders f WHERE {FILTRO_F} AND f.is_delivered""").iloc[0]["cobertura_pct"]
    st.metric("Entregas com avaliação", f"{float(cob or 0):.1f}%")

# ------------------------------------------------------------------ qualidade
with abas[6]:
    st.subheader("Relatório de qualidade da camada silver")
    st.dataframe(consulta(f"SELECT * FROM {T}.silver_quality_report ORDER BY step, rule"), use_container_width=True)
