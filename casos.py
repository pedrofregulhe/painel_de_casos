import streamlit as st
import pandas as pd
from datetime import datetime
import os
import plotly.express as px
from io import BytesIO

# 1. Configuração da Página - Sidebar recolhida por padrão
st.set_page_config(page_title="Painel de Casos - Culligan", layout="wide", initial_sidebar_state="collapsed")

# --- CUSTOM CSS (Visual Clean & Minimalista) ---
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Configuração Geral do Fundo e Container */
    .stApp { background-color: #f8fafd; }
    .block-container { padding-top: 1.5rem !important; padding-bottom: 2rem !important; }
    
    /* Estilização do Topo */
    .header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1.5rem;
        background-color: #ffffff;
        padding: 15px 25px;
        border-radius: 12px;
        box-shadow: 0 2px 12px rgba(0, 94, 184, 0.04);
    }
    
    /* Grid de Filtros Horizontal */
    .filter-box {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.02);
        margin-bottom: 1.5rem;
    }
    
    /* Cards de KPI Reestilizados */
    div.stMetric { display: none; }
    .kpi-container {
        display: flex;
        flex-wrap: wrap;
        gap: 16px;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background-color: #ffffff;
        padding: 22px 16px; 
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
        flex: 1 1 12%; 
        min-width: 140px;
        text-align: center;
        border-top: 4px solid #b0c4de;
        transition: all 0.25s ease-in-out;
    }
    .kpi-card:hover { 
        transform: translateY(-4px); 
        box-shadow: 0 8px 20px rgba(0, 94, 184, 0.08);
    }
    
    /* Cores de Alerta Discretas nas Bordas Superiores */
    .kpi-card.padrao { border-top-color: #005eb8; }
    .kpi-card.atraso { border-top-color: #e74c3c; }
    .kpi-card.alerta { border-top-color: #f39c12; }
    .kpi-card.sucesso { border-top-color: #27ae60; }
    
    .kpi-title {
        font-size: 11px; 
        color: #8a99a8;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 8px;
    }
    .kpi-value {
        font-size: 30px !important; 
        color: #1e293b;
        font-weight: 700; 
        margin: 0;
        line-height: 1.1;
    }
    
    /* Customização das Abas nativas do Streamlit */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        margin-bottom: 1.5rem;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        padding: 10px 20px;
        border-radius: 8px;
        font-weight: 600;
        color: #64748b;
        transition: all 0.2s;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #005eb8;
        border-color: #005eb8;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background-color: #005eb8 !important;
        color: white !important;
        border-color: #005eb8;
    }
    </style>
""", unsafe_allow_html=True)

# 2. Carregar Dados
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
        df.loc[df['SLA_Dinâmico'].str.contains('atras', case=False, na=False), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[df['SLA_Dinâmico'].str.contains('prazo', case=False, na=False), 'SLA_Dinâmico'] = '🟢 No Prazo'

        mask_fechado = df.get('Status', '').str.lower().isin(['fechado', 'closed'])
        df.loc[mask_fechado, 'SLA_Dinâmico'] = '⚪ Fechado'

        mask_aberto = ~mask_fechado
        mask_tem_data = df['Última Interação DT'].notna()
        fila_upper = df.get('Fila Principal', '').astype(str).str.upper()
        
        mask_corpo = fila_upper.str.contains("CORPORATIVO", na=False)
        mask_gen = fila_upper.str.contains("GENÉRICO", na=False)
        mask_nps = fila_upper.str.contains("NPS", na=False)

        agora = pd.Timestamp.now()
        horas_passadas = (agora - df['Última Interação DT']).dt.total_seconds() / 3600

        df.loc[mask_aberto & mask_tem_data & mask_corpo & (horas_passadas > 48), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_corpo & (horas_passadas <= 48), 'SLA_Dinâmico'] = '🟢 No Prazo'
        df.loc[mask_aberto & mask_tem_data & mask_gen & (horas_passadas > 24), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_gen & (horas_passadas <= 24), 'SLA_Dinâmico'] = '🟢 No Prazo'
        df.loc[mask_aberto & mask_tem_data & mask_nps & (horas_passadas > 24), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_nps & (horas_passadas <= 24), 'SLA_Dinâmico'] = '🟢 No Prazo'

        df['SLA Estipulado'] = df.get('Regra SLA SF', 'Regra Salesforce')
        df.loc[mask_corpo, 'SLA Estipulado'] = '48h'
        df.loc[mask_gen, 'SLA Estipulado'] = '24h'
        df.loc[mask_nps, 'SLA Estipulado'] = '24h'

        return df
    except FileNotFoundError:
        st.error("Arquivo 'Base_OA_PowerBI.csv' não encontrado.")
        return pd.DataFrame()

df_completo = carregar_dados()

if not df_completo.empty:
    
    # --- TOPO DA PÁGINA (LOGO E ATUALIZAÇÃO REUNIDOS) ---
    try:
        with open("data_hora_atualização.txt", "r", encoding="utf-8") as file:
            data_hora_txt = file.read().strip()
    except FileNotFoundError:
        data_hora_txt = "Desconhecida"

    st.markdown(f"""
        <div class="header-container">
            <div style="font-size: 24px; font-weight: 700; color: #005eb8;">Painel de Casos</div>
            <div style="text-align: right; font-size: 13px; color: #64748b;">
                Última atualização: <strong style="color: #1e293b;">{data_hora_txt}</strong>
            </div>
        </div>
    """, unsafe_allow_html=True)

    # --- BARRA DE FILTROS HORIZONTAL NO CORPO ---
    st.markdown('<div class="filter-box">', unsafe_allow_html=True)
    
    col_f1, col_f2, col_f3, col_f4 = st.columns([1.2, 1.5, 1.5, 1.2])
    df_filtrado = df_completo.copy()

    with col_f1:
        if 'Abertura Data' in df_filtrado.columns:
            min_date = df_filtrado['Abertura Data'].min()
            max_date = df_filtrado['Abertura Data'].max()
            hoje = pd.Timestamp.now().date()
            primeiro_dia_mes = hoje.replace(day=1)
            
            start_default = max(min_date, primeiro_dia_mes) if pd.notna(min_date) else primeiro_dia_mes
            end_default = min(max_date, hoje) if pd.notna(max_date) else hoje

            if pd.notna(min_date) and pd.notna(max_date):
                datas = st.date_input("Período de Abertura:", [start_default, end_default], min_value=min_date, max_value=max_date, format="DD/MM/YYYY")
                if len(datas) == 2:
                    df_filtrado = df_filtrado[(df_filtrado['Abertura Data'] >= datas[0]) & (df_filtrado['Abertura Data'] <= datas[1])]

    with col_f2:
        if 'Status' in df_filtrado.columns:
            lista_status = sorted(df_completo['Status'].dropna().unique().tolist())
            status_selecionados = st.multiselect("Status do Caso:", lista_status, default=[], placeholder="Vazio = Todos os Abertos")
            
            if len(status_selecionados) > 0:
                df_filtrado = df_filtrado[df_filtrado['Status'].isin(status_selecionados)]
            else:
                df_filtrado = df_filtrado[~df_filtrado['Status'].str.lower().isin(['fechado', 'closed'])]

    with col_f3:
        if 'Fila Principal' in df_filtrado.columns:
            lista_filas = sorted(df_completo['Fila Principal'].dropna().unique().tolist())
            if "NPS" not in lista_filas:
                lista_filas.append("NPS")
                lista_filas = sorted(lista_filas)
            
            filas_selecionadas = st.multiselect("Fila Principal:", lista_filas, default=[], placeholder="Vazio = Todas as Filas")
            if len(filas_selecionadas) > 0:
                df_filtrado = df_filtrado[df_filtrado['Fila Principal'].isin(filas_selecionadas)]

    with col_f4:
        if 'Subfila' in df_filtrado.columns:
            lista_subfilas = sorted(df_completo['Subfila'].dropna().unique().tolist())
            subfilas_selecionadas = st.multiselect("Subfila (Opcional):", lista_subfilas, default=[], placeholder="Vazio = Todas as Carteiras")
            if len(subfilas_selecionadas) > 0:
                df_filtrado = df_filtrado[df_filtrado['Subfila'].isin(subfilas_selecionadas)]
                
    st.markdown('</div>', unsafe_allow_html=True)

    # --- PROCESSAMENTO DOS BLOCOS DE MÉTRICAS ---
    total_casos = len(df_filtrado)
    df_abertos = df_filtrado[df_filtrado['Status'].str.lower().isin(['aberto', 'em aberto', 'novo'])] if 'Status' in df_filtrado.columns else pd.DataFrame()
    df_fechados = df_filtrado[df_filtrado['Status'].str.lower().isin(['fechado', 'closed'])] if 'Status' in df_filtrado.columns else pd.DataFrame()
    
    abertos = len(df_abertos)
    fechados = len(df_fechados)
    em_tratativa = total_casos - fechados - abertos 
    atrasados = len(df_filtrado[df_filtrado['SLA_Dinâmico'].str.contains("Atraso", na=False)])

    idade_media_txt = f"{df_abertos['Idade (Dias)'].mean():.1f}" if not df_abertos.empty and 'Idade (Dias)' in df_abertos.columns and pd.notna(df_abertos['Idade (Dias)'].mean()) else "0"
    if not df_fechados.empty and 'Fechamento DT' in df_fechados.columns and 'Abertura DT' in df_fechados.columns:
        tmt_txt = f"{((df_fechados['Fechamento DT'] - df_fechados['Abertura DT']).dt.total_seconds() / 86400).mean():.1f}"
    else:
        tmt_txt = "0"

    # Container de Métricas
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-card padrao"><div class="kpi-title">Total Casos</div><p class="kpi-value">{total_casos:,}</p></div>
        <div class="kpi-card padrao"><div class="kpi-title">Abertos</div><p class="kpi-value">{abertos:,}</p></div>
        <div class="kpi-card padrao"><div class="kpi-title">Em Tratativa</div><p class="kpi-value">{em_tratativa:,}</p></div>
        <div class="kpi-card atraso"><div class="kpi-title" style="color: #c0392b;">SLA Atrasado</div><p class="kpi-value" style="color: #e74c3c;">{atrasados:,}</p></div>
        <div class="kpi-card alerta"><div class="kpi-title">Idade Média</div><p class="kpi-value" style="color: #f39c12;">{idade_media_txt}<span style="font-size:14px; font-weight:500;"> d</span></p></div>
        <div class="kpi-card sucesso"><div class="kpi-title">Tempo Médio</div><p class="kpi-value" style="color: #27ae60;">{tmt_txt}<span style="font-size:14px; font-weight:500;"> d</span></p></div>
        <div class="kpi-card padrao"><div class="kpi-title">Fechados</div><p class="kpi-value">{fechados:,}</p></div>
    </div>
    """.replace(",", "."), unsafe_allow_html=True)

    # --- DIVISÃO EM ABAS OPERACIONAIS ---
    tab_graficos, tab_tabela = st.tabs(["📊 Indicadores & Gráficos", "📋 Extrato Detalhado dos Casos"])

    with tab_graficos:
        st.markdown("<br>", unsafe_allow_html=True)
        graf_col1, graf_col2 = st.columns([1, 1])

        with graf_col1:
            st.markdown("<h4 style='color: #1e293b; font-size:16px;'>Visão Geral de Conformidade do SLA</h4>", unsafe_allow_html=True)
            if 'SLA_Dinâmico' in df_filtrado.columns and not df_filtrado.empty:
                sla_counts = df_filtrado['SLA_Dinâmico'].value_counts().reset_index()
                sla_counts.columns = ['Status SLA', 'Quantidade']
                mapa_cores = {'🔴 Em Atraso': '#e74c3c', '🟢 No Prazo': '#2ecc71', '⚪ Fechado': '#94a3b8'}
                
                fig_donut = px.pie(sla_counts, names='Status SLA', values='Quantidade', hole=0.6, color='Status SLA', color_discrete_map=mapa_cores)
                fig_donut.update_traces(textposition='inside', textinfo='percent+label')
                fig_donut.update_layout(margin=dict(t=20, b=10, l=10, r=10), showlegend=False, height=320, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_donut, use_container_width=True)

        with graf_col2:
            st.markdown("<h4 style='color: #1e293b; font-size:16px;'>Top 10 Carteiras por Volume</h4>", unsafe_allow_html=True)
            if 'Subfila' in df_filtrado.columns and not df_filtrado.empty:
                df_filas_validas = df_filtrado[df_filtrado['Subfila'] != '-']
                fila_counts = df_filas_validas['Subfila'].value_counts().reset_index().head(10)
                fila_counts.columns = ['Carteira', 'Volume']
                
                fig_bar = px.bar(fila_counts, x='Volume', y='Carteira', orientation='h', text='Volume', color_discrete_sequence=['#005eb8'])
                fig_bar.update_traces(textposition='outside')
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=20, b=10, l=10, r=10), height=320, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_bar, use_container_width=True)

    with tab_tabela:
        st.markdown("<br>", unsafe_allow_html=True)
        
        colunas_base = [
            'Número', 'Link Salesforce', 'Abertura', 'Fechamento', 'Origem (Fila Anterior)', 'Quem Aceitou', 'Quem Fechou', 'Fila Principal', 'Subfila', 
            'Qtd Interações (E-mails)', 'Última Interação', 'SLA Estipulado', 'SLA_Dinâmico', 'Conta'
        ]
        colunas_existentes = [c for c in colunas_base if c in df_filtrado.columns]
        df_tabela = df_filtrado[colunas_existentes].copy()

        nomes_colunas = {
            'Número': 'Caso', 'Link Salesforce': 'SalesForce', 'Origem (Fila Anterior)': 'Veio de', 
            'Fila Principal': 'Fila', 'Qtd Interações (E-mails)': 'Interações', 'Conta': 'Cliente'
        }
        df_tabela.rename(columns=nomes_colunas, inplace=True)

        ext_col1, ext_col2 = st.columns([3, 1])
        with ext_col1:
            st.markdown("<h4 style='color: #1e293b; font-size:18px; margin-top:5px;'>Extrato de Dados Filtrados</h4>", unsafe_allow_html=True)
        with ext_col2:
            def convert_df_to_excel(df):
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Extrato')
                return output.getvalue()

            if not df_tabela.empty:
                excel_data = convert_df_to_excel(df_tabela)
                st.download_button(
                    label="📥 Exportar Excel",
                    data=excel_data,
                    file_name=f"Extrato_Culligan_{datetime.now().strftime('%d%m%Y')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

        config_colunas = {"SalesForce": st.column_config.LinkColumn("SalesForce", display_text="🔗 Abrir")}
        st.dataframe(df_tabela, use_container_width=True, hide_index=True, column_config=config_colunas, height=400)