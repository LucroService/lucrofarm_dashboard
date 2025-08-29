# radar_dashboard.py

import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(page_title="LucroFarmEvolucion", layout="wide")

dados = {
    "Prospecção (emails válidos)": {"atual": 4, "meta": 8},
    "Validação (qualidade emails)": {"atual": 6, "meta": 9},
    "Automação IA": {"atual": 6, "meta": 9},
    "Escalabilidade (farm + equipe)": {"atual": 5, "meta": 8},
    "Custo (lucro líquido)": {"atual": 4, "meta": 9},
    "Parceria Profissional (divisão lucro)": {"atual": 5, "meta": 9},
    "Volume de Leads Diários": {"atual": 3, "meta": 7},
}

labels = list(dados.keys())
valores_atuais = [dados[k]["atual"] for k in labels]
valores_meta = [dados[k]["meta"] for k in labels]

st.title("🚀 LucroFarmEvolucion")
st.caption(f"Atualizado em {datetime.now().strftime('%d/%m/%Y')}")

fig = go.Figure()

fig.add_trace(go.Scatterpolar(
    r=valores_meta + [valores_meta[0]],
    theta=labels + [labels[0]],
    fill='toself',
    name='Meta',
    line_color="purple",
    opacity=0.5
))

fig.add_trace(go.Scatterpolar(
    r=valores_atuais + [valores_atuais[0]],
    theta=labels + [labels[0]],
    fill='toself',
    name='Atual',
    line_color="limegreen",
    opacity=0.7
))

fig.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
    showlegend=True,
    template="plotly_dark"
)

st.plotly_chart(fig, use_container_width=True)

st.subheader("📊 Status por Área")
for i, label in enumerate(labels):
    atual = valores_atuais[i]
    meta = valores_meta[i]
    delta = atual - meta
    if delta >= 0:
        status = "✅"
    elif delta >= -2:
        status = "⚠️"
    else:
        status = "❌"
    st.write(f"- **{label}** → Atual: {atual} | Meta: {meta} | Status: {status} ({delta:+d})")
