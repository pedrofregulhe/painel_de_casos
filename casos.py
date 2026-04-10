import streamlit as st
import pandas as pd
from datetime import datetime
import os
import plotly.express as px
from io import BytesIO

# 1. Configuração da Página
st.set_page_config(page_title="Painel de Casos - Culligan", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    /* ESCONDE O CABEÇALHO E RODAPÉ PADRÃO DO STREAMLIT */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* SOBE O CONTEÚDO */
    .block-container {
        padding-top: 2rem !important; 
    }
    
    div.stMetric { display: none; }
    .kpi-container {
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 30px;
        margin-top: 0px; 
    }
    .kpi-card {
        background-color: #ffffff;
        padding: 20px 10px; 
        border-radius: 10px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.08);
        flex: 1 1 13%; 
        min-width: 130px;
        text-align: center;
        border-left: 6px solid #005eb8;
        transition: transform 0.2s ease-in-out;
    }
    .kpi-card:hover { transform: translateY(-5px); }
    .kpi-card.atraso { border-left: 6px solid #e74c3c; }
    .kpi-card.alerta { border-left: 6px solid #f39c12; }
    .kpi-card.sucesso { border-left: 6px solid #27ae60; }
    .kpi-title {
        font-size: 12px; 
        color: #7f8c8d;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 8px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .kpi-value {
        font-size: 32px !important; /* <--- Reduzido para 32px, bem mais elegante */
        color: #2c3e50;
        font-weight: 700; /* Levemente menos espesso */
        margin: 0;
        line-height: 1.2; 
    }
    .stApp { background-color: #f8f9fa; }
    </style>
""", unsafe_allow_html=True)

# 2. Carregar e Processar os Dados
@st.cache_data(ttl=600)
def carregar_dados():
    try:
        df = pd.read_csv('Base_OA_PowerBI.csv', encoding='utf-8-sig')
        
        if 'Abertura' in df.columns:
            df['Abertura DT'] = pd.to_datetime(df['Abertura'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
            df['Abertura Data'] = df['Abertura DT'].dt.date
        if 'Fechamento' in df.columns:
            df['Fechamento DT'] = pd.to_datetime(df['Fechamento'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        if 'Última Interação' in df.columns:
            df['Última Interação DT'] = pd.to_datetime(df['Última Interação'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        if 'Idade (Dias)' in df.columns:
            df['Idade (Dias)'] = pd.to_numeric(df['Idade (Dias)'], errors='coerce')

        df['SLA_Dinâmico'] = df.get('SLA Macro', 'No Prazo').astype(str)
        mask_atraso_orig = df['SLA_Dinâmico'].str.contains('atras', case=False, na=False)
        mask_prazo_orig = df['SLA_Dinâmico'].str.contains('prazo', case=False, na=False)
        df.loc[mask_atraso_orig, 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_prazo_orig, 'SLA_Dinâmico'] = '🟢 No Prazo'

        mask_fechado = df.get('Status', '').str.lower().isin(['fechado', 'closed'])
        df.loc[mask_fechado, 'SLA_Dinâmico'] = '⚪ Fechado'

        mask_aberto = ~mask_fechado
        mask_tem_data = df['Última Interação DT'].notna()
        fila_upper = df.get('Fila Principal', '').astype(str).str.upper()
        mask_corpo = fila_upper.str.contains("CORPORATIVO", na=False)
        mask_gen = fila_upper.str.contains("GENÉRICO", na=False)

        agora = pd.Timestamp.now()
        horas_passadas = (agora - df['Última Interação DT']).dt.total_seconds() / 3600

        df.loc[mask_aberto & mask_tem_data & mask_corpo & (horas_passadas > 48), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_corpo & (horas_passadas <= 48), 'SLA_Dinâmico'] = '🟢 No Prazo'

        df.loc[mask_aberto & mask_tem_data & mask_gen & (horas_passadas > 24), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_gen & (horas_passadas <= 24), 'SLA_Dinâmico'] = '🟢 No Prazo'

        df['SLA Estipulado'] = df.get('Regra SLA SF', 'Regra Salesforce')
        df.loc[mask_corpo, 'SLA Estipulado'] = '48h'
        df.loc[mask_gen, 'SLA Estipulado'] = '24h'

        return df
    except FileNotFoundError:
        st.error("Arquivo 'Base_OA_PowerBI.csv' não encontrado.")
        return pd.DataFrame()

df_completo = carregar_dados()

if not df_completo.empty:
    
    # --- BARRA LATERAL ---
    try:
        st.sidebar.image("logo.png", width=180) # <-- Ajuste este número (ex: 80, 100, 120) para o tamanho ideal
    except FileNotFoundError:
        st.sidebar.warning("⚠️ 'logo.png' não encontrado.")

    try:
        with open("data_hora_atualização.txt", "r", encoding="utf-8") as file:
            data_hora_txt = file.read().strip()
        st.sidebar.markdown(f"<p style='text-align: center; font-size: 13px; color: gray; margin-top: -10px;'>Última atualização:<br><b>{data_hora_txt}</b></p>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.sidebar.markdown("<p style='text-align: center; font-size: 13px; color: gray; margin-top: -10px;'>Última atualização:<br><b>Desconhecida</b></p>", unsafe_allow_html=True)
    
    if st.sidebar.button("🔄 Limpar Filtros", use_container_width=True):
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Filtros")

    df_filtrado = df_completo.copy()

    with st.sidebar.expander("📅 Período de Abertura", expanded=True):
        if 'Abertura Data' in df_filtrado.columns:
            min_date = df_filtrado['Abertura Data'].min()
            max_date = df_filtrado['Abertura Data'].max()
            if pd.notna(min_date) and pd.notna(max_date):
                datas = st.date_input("Selecione o intervalo:", [min_date, max_date], min_value=min_date, max_value=max_date, format="DD/MM/YYYY")
                if len(datas) == 2:
                    df_filtrado = df_filtrado[(df_filtrado['Abertura Data'] >= datas[0]) & (df_filtrado['Abertura Data'] <= datas[1])]

    with st.sidebar.expander("📌 Status dos Casos", expanded=True):
        if 'Status' in df_filtrado.columns:
            lista_status = sorted(df_filtrado['Status'].dropna().unique().tolist())
            status_selecionados = st.multiselect("Selecione o Status:", lista_status, default=lista_status)
            if len(status_selecionados) > 0:
                df_filtrado = df_filtrado[df_filtrado['Status'].isin(status_selecionados)]

    with st.sidebar.expander("🏢 Filas e Carteiras", expanded=True):
        if 'Fila Principal' in df_filtrado.columns:
            lista_filas = sorted(df_filtrado['Fila Principal'].dropna().unique().tolist())
            filas_selecionadas = st.multiselect("Fila Principal:", lista_filas, default=lista_filas)
            if len(filas_selecionadas) > 0:
                df_filtrado = df_filtrado[df_filtrado['Fila Principal'].isin(filas_selecionadas)]

        if 'Subfila' in df_filtrado.columns:
            lista_subfilas = sorted(df_filtrado['Subfila'].dropna().unique().tolist())
            subfilas_selecionadas = st.multiselect("Subfila (Opcional):", lista_subfilas, default=[], help="Vazio mostra todas")
            if len(subfilas_selecionadas) > 0:
                df_filtrado = df_filtrado[df_filtrado['Subfila'].isin(subfilas_selecionadas)]

    # --- TELA PRINCIPAL (KPIs) ---
    total_casos = len(df_filtrado)
    df_abertos = df_filtrado[df_filtrado['Status'].str.lower().isin(['aberto', 'em aberto', 'novo'])] if 'Status' in df_filtrado.columns else pd.DataFrame()
    df_fechados = df_filtrado[df_filtrado['Status'].str.lower().isin(['fechado', 'closed'])] if 'Status' in df_filtrado.columns else pd.DataFrame()
    
    abertos = len(df_abertos)
    fechados = len(df_fechados)
    em_tratativa = total_casos - fechados - abertos 
    atrasados = len(df_filtrado[df_filtrado['SLA_Dinâmico'].str.contains("Atraso", na=False)])

    # Idade Média (Abertos)
    if not df_abertos.empty and 'Idade (Dias)' in df_abertos.columns:
        idade_media = df_abertos['Idade (Dias)'].mean()
        idade_media_txt = f"{idade_media:.1f}" if pd.notna(idade_media) else "0"
    else:
        idade_media_txt = "0"

    # Tempo Médio de Tratativa (Fechados)
    if not df_fechados.empty and 'Fechamento DT' in df_fechados.columns and 'Abertura DT' in df_fechados.columns:
        diff_dias = (df_fechados['Fechamento DT'] - df_fechados['Abertura DT']).dt.total_seconds() / 86400
        tmt_media = diff_dias.mean()
        tmt_txt = f"{tmt_media:.1f}" if pd.notna(tmt_media) else "0"
    else:
        tmt_txt = "0"

    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-card"><div class="kpi-title">Total Casos</div><p class="kpi-value">{total_casos:,}</p></div>
        <div class="kpi-card"><div class="kpi-title">Abertos</div><p class="kpi-value">{abertos:,}</p></div>
        <div class="kpi-card"><div class="kpi-title">Em Tratativa</div><p class="kpi-value">{em_tratativa:,}</p></div>
        <div class="kpi-card atraso"><div class="kpi-title">SLA Atrasado</div><p class="kpi-value" style="color: #c0392b;">{atrasados:,}</p></div>
        <div class="kpi-card alerta" title="Idade média dos casos que ainda estão abertos"><div class="kpi-title">Idade Média</div><p class="kpi-value" style="color: #e67e22;">{idade_media_txt}<span style="font-size:14px;"> d</span></p></div>
        <div class="kpi-card sucesso" title="Tempo Médio de Tratativa dos casos finalizados"><div class="kpi-title">Tempo Médio</div><p class="kpi-value" style="color: #27ae60;">{tmt_txt}<span style="font-size:14px;"> d</span></p></div>
        <div class="kpi-card"><div class="kpi-title">Fechados</div><p class="kpi-value">{fechados:,}</p></div>
    </div>
    """.replace(",", "."), unsafe_allow_html=True)


    # --- TABELA DE DADOS E EXPORTAÇÃO ---
    st.markdown("---")
    
    colunas_base = [
        'Número', 'Link Salesforce', 'Abertura', 'Fechamento', 'Fila Principal', 'Subfila', 
        'Qtd Interações (E-mails)', 'Última Interação', 'SLA Estipulado', 'SLA_Dinâmico', 'Conta'
    ]
    colunas_existentes = [c for c in colunas_base if c in df_filtrado.columns]
    df_tabela = df_filtrado[colunas_existentes].copy()

    nomes_colunas = {
        'Número': 'Caso', 'Link Salesforce': 'SalesForce', 'Fila Principal': 'Fila',
        'Qtd Interações (E-mails)': 'Qtd de Interações', 'Conta': 'Cliente'
    }
    df_tabela.rename(columns=nomes_colunas, inplace=True)

    tab_col1, tab_col2 = st.columns([3, 1])
    with tab_col1:
        st.subheader("Extrato de Casos")
    with tab_col2:
        def convert_df_to_excel(df):
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Extrato')
            return output.getvalue()

        if not df_tabela.empty:
            excel_data = convert_df_to_excel(df_tabela)
            st.download_button(
                label="📥 Baixar Extrato (Excel)",
                data=excel_data,
                file_name=f"Extrato_Culligan_{datetime.now().strftime('%d%m%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    config_colunas = {"SalesForce": st.column_config.LinkColumn("SalesForce", display_text="🔗 Abrir Caso")}
    st.dataframe(df_tabela, use_container_width=True, hide_index=True, column_config=config_colunas, height=350)


    # --- GRÁFICOS VISUAIS ---
    st.markdown("---")
    graf_col1, graf_col2 = st.columns([1, 1])

    with graf_col1:
        st.markdown("**📊 Visão Geral do SLA**")
        if 'SLA_Dinâmico' in df_filtrado.columns and not df_filtrado.empty:
            sla_counts = df_filtrado['SLA_Dinâmico'].value_counts().reset_index()
            sla_counts.columns = ['Status SLA', 'Quantidade']
            
            mapa_cores = {'🔴 Em Atraso': '#e74c3c', '🟢 No Prazo': '#2ecc71', '⚪ Fechado': '#bdc3c7'}
            fig_donut = px.pie(sla_counts, names='Status SLA', values='Quantidade', hole=0.6,
                               color='Status SLA', color_discrete_map=mapa_cores)
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            
            fig_donut.update_layout(
                margin=dict(t=0, b=0, l=0, r=0), 
                showlegend=False, 
                height=300,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_donut, use_container_width=True)

    with graf_col2:
        st.markdown("**📈 Top 10 Carteiras (Volume)**")
        if 'Subfila' in df_filtrado.columns and not df_filtrado.empty:
            df_filas_validas = df_filtrado[df_filtrado['Subfila'] != '-']
            fila_counts = df_filas_validas['Subfila'].value_counts().reset_index().head(10)
            fila_counts.columns = ['Carteira', 'Volume']
            
            fig_bar = px.bar(fila_counts, x='Volume', y='Carteira', orientation='h', text='Volume',
                             color_discrete_sequence=['#005eb8'])
            fig_bar.update_traces(textposition='outside')
            
            fig_bar.update_layout(
                yaxis={'categoryorder':'total ascending'}, 
                margin=dict(t=0, b=0, l=0, r=0), 
                height=300,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_bar, use_container_width=True)