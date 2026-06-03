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
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    .block-container {
        padding-top: 2rem !important; 
    }
    
    div.stMetric { display: none; }
    
    /* Melhoria no visual dos filtros na barra lateral */
    [data-testid="stSidebar"] div.stMarkdown p {
        font-size: 14px;
        font-weight: 500;
        color: #333;
    }
    
    /* Ajuste de espaçamento e sombra nos KPIs para um design mais limpo */
    .kpi-container {
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        gap: 15px;
        margin-bottom: 30px;
        margin-top: 10px; 
    }
    .kpi-card {
        background-color: #ffffff;
        padding: 20px 15px; 
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        flex: 1 1 13%; 
        min-width: 130px;
        text-align: center;
        border-left: 5px solid #005eb8;
        transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
    }
    .kpi-card:hover { 
        transform: translateY(-3px); 
        box-shadow: 0 6px 15px rgba(0,0,0,0.1);
    }
    .kpi-card.atraso { border-left-color: #e74c3c; }
    .kpi-card.alerta { border-left-color: #f39c12; }
    .kpi-card.sucesso { border-left-color: #27ae60; }
    .kpi-title {
        font-size: 13px; 
        color: #555;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 10px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .kpi-value {
        font-size: 28px !important; 
        color: #2c3e50;
        font-weight: 700; 
        margin: 0;
    }
    .stApp { background-color: #f4f6f8; }
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

        # --- TRADUÇÃO DE IDs DO SALESFORCE ---
        # Adicione os IDs e os nomes reais aqui conforme for identificando no extrato
        mapeamento_sf = {
            '005N500000SgE4sIAF': 'Nome do Atendente 1',
            '005XXXXXXXXXXXXXXX': 'Nome do Atendente 2'
        }
        if 'Origem (Fila Anterior)' in df.columns:
            df['Origem (Fila Anterior)'] = df['Origem (Fila Anterior)'].replace(mapeamento_sf)

        # Regras de SLA e Status
        df['SLA_Dinâmico'] = df.get('SLA Macro', 'No Prazo').astype(str)
        df.loc[df['SLA_Dinâmico'].str.contains('atras', case=False, na=False), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[df['SLA_Dinâmico'].str.contains('prazo', case=False, na=False), 'SLA_Dinâmico'] = '🟢 No Prazo'

        mask_fechado = df.get('Status', '').str.lower().isin(['fechado', 'closed'])
        df.loc[mask_fechado, 'SLA_Dinâmico'] = '⚪ Fechado'

        # Regras Específicas de Filas
        mask_aberto = ~mask_fechado
        mask_tem_data = df['Última Interação DT'].notna()
        fila_upper = df.get('Fila Principal', '').astype(str).str.upper()
        
        mask_corpo = fila_upper.str.contains("CORPORATIVO", na=False)
        mask_gen = fila_upper.str.contains("GENÉRICO", na=False)
        mask_nps = fila_upper.str.contains("NPS", na=False) # Nova fila NPS

        agora = pd.Timestamp.now()
        horas_passadas = (agora - df['Última Interação DT']).dt.total_seconds() / 3600

        # Aplicação de SLA por Fila
        df.loc[mask_aberto & mask_tem_data & mask_corpo & (horas_passadas > 48), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_corpo & (horas_passadas <= 48), 'SLA_Dinâmico'] = '🟢 No Prazo'

        df.loc[mask_aberto & mask_tem_data & mask_gen & (horas_passadas > 24), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_gen & (horas_passadas <= 24), 'SLA_Dinâmico'] = '🟢 No Prazo'
        
        # SLA para NPS (Estipulado em 24h por padrão, ajuste se necessário)
        df.loc[mask_aberto & mask_tem_data & mask_nps & (horas_passadas > 24), 'SLA_Dinâmico'] = '🔴 Em Atraso'
        df.loc[mask_aberto & mask_tem_data & mask_nps & (horas_passadas <= 24), 'SLA_Dinâmico'] = '🟢 No Prazo'

        df['SLA Estipulado'] = df.get('Regra SLA SF', 'Regra Salesforce')
        df.loc[mask_corpo, 'SLA Estipulado'] = '48h'
        df.loc[mask_gen, 'SLA Estipulado'] = '24h'
        df.loc[mask_nps, 'SLA Estipulado'] = '24h' # SLA Base para exibição

        return df
    except FileNotFoundError:
        st.error("Arquivo 'Base_OA_PowerBI.csv' não encontrado.")
        return pd.DataFrame()

df_completo = carregar_dados()

if not df_completo.empty:
    
    # --- BARRA LATERAL (DESIGN LIMPO) ---
    col1, col_logo, col2 = st.sidebar.columns([1, 3, 1]) 
    with col_logo:
        try:
            st.image("logo.png", use_container_width=True)
        except FileNotFoundError:
            st.sidebar.markdown("### Culligan")

    try:
        with open("data_hora_atualização.txt", "r", encoding="utf-8") as file:
            data_hora_txt = file.read().strip()
        st.sidebar.markdown(f"<p style='text-align: center; font-size: 12px; color: gray; margin-top: -10px;'>Atualizado em: <b>{data_hora_txt}</b></p>", unsafe_allow_html=True)
    except:
        pass
    
    if st.sidebar.button("🔄 Limpar Filtros", use_container_width=True):
        st.rerun()

    st.sidebar.markdown("---")
    
    df_filtrado = df_completo.copy()

    # 1. Filtro de Data (Mês Atual por Padrão)
    st.sidebar.markdown("📅 **Período de Abertura**")
    if 'Abertura Data' in df_filtrado.columns:
        min_date = df_filtrado['Abertura Data'].min()
        max_date = df_filtrado['Abertura Data'].max()
        
        hoje = pd.Timestamp.now().date()
        primeiro_dia_mes = hoje.replace(day=1)
        
        # Evita erro caso a base não tenha dados do mês atual ainda
        start_default = max(min_date, primeiro_dia_mes) if pd.notna(min_date) else primeiro_dia_mes
        end_default = min(max_date, hoje) if pd.notna(max_date) else hoje

        if pd.notna(min_date) and pd.notna(max_date):
            datas = st.sidebar.date_input("Intervalo:", [start_default, end_default], min_value=min_date, max_value=max_date, format="DD/MM/YYYY")
            if len(datas) == 2:
                df_filtrado = df_filtrado[(df_filtrado['Abertura Data'] >= datas[0]) & (df_filtrado['Abertura Data'] <= datas[1])]
    
    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    # 2. Filtro de Status (Excluindo Fechados por Padrão)
    st.sidebar.markdown("📌 **Status dos Casos**")
    if 'Status' in df_filtrado.columns:
        lista_status = sorted(df_completo['Status'].dropna().unique().tolist())
        # Filtra a lista padrão tirando fechados
        status_padrao = [s for s in lista_status if s.lower() not in ['fechado', 'closed']]
        
        status_selecionados = st.sidebar.multiselect("Status:", lista_status, default=status_padrao)
        if len(status_selecionados) > 0:
            df_filtrado = df_filtrado[df_filtrado['Status'].isin(status_selecionados)]

    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    # 3. Filtro de Filas (Injetando NPS se não existir na base atual)
    st.sidebar.markdown("🏢 **Filas e Carteiras**")
    if 'Fila Principal' in df_filtrado.columns:
        lista_filas = sorted(df_completo['Fila Principal'].dropna().unique().tolist())
        if "NPS" not in lista_filas:
            lista_filas.append("NPS")
            lista_filas = sorted(lista_filas)
            
        filas_selecionadas = st.sidebar.multiselect("Fila Principal:", lista_filas, default=lista_filas)
        if len(filas_selecionadas) > 0:
            df_filtrado = df_filtrado[df_filtrado['Fila Principal'].isin(filas_selecionadas)]

    if 'Subfila' in df_filtrado.columns:
        lista_subfilas = sorted(df_completo['Subfila'].dropna().unique().tolist())
        subfilas_selecionadas = st.sidebar.multiselect("Subfila (Opcional):", lista_subfilas, default=[], help="Vazio mostra todas")
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

    # Idade Média
    idade_media_txt = f"{df_abertos['Idade (Dias)'].mean():.1f}" if not df_abertos.empty and 'Idade (Dias)' in df_abertos.columns and pd.notna(df_abertos['Idade (Dias)'].mean()) else "0"
    
    # TMT
    if not df_fechados.empty and 'Fechamento DT' in df_fechados.columns and 'Abertura DT' in df_fechados.columns:
        tmt_media = ((df_fechados['Fechamento DT'] - df_fechados['Abertura DT']).dt.total_seconds() / 86400).mean()
        tmt_txt = f"{tmt_media:.1f}" if pd.notna(tmt_media) else "0"
    else:
        tmt_txt = "0"

    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-card"><div class="kpi-title">Total Casos</div><p class="kpi-value">{total_casos:,}</p></div>
        <div class="kpi-card"><div class="kpi-title">Abertos</div><p class="kpi-value">{abertos:,}</p></div>
        <div class="kpi-card"><div class="kpi-title">Em Tratativa</div><p class="kpi-value">{em_tratativa:,}</p></div>
        <div class="kpi-card atraso"><div class="kpi-title">SLA Atrasado</div><p class="kpi-value" style="color: #e74c3c;">{atrasados:,}</p></div>
        <div class="kpi-card alerta" title="Idade média dos abertos"><div class="kpi-title">Idade Média</div><p class="kpi-value" style="color: #f39c12;">{idade_media_txt}<span style="font-size:16px;">d</span></p></div>
        <div class="kpi-card sucesso" title="TMT dos finalizados"><div class="kpi-title">Tempo Médio</div><p class="kpi-value" style="color: #27ae60;">{tmt_txt}<span style="font-size:16px;">d</span></p></div>
        <div class="kpi-card"><div class="kpi-title">Fechados</div><p class="kpi-value">{fechados:,}</p></div>
    </div>
    """.replace(",", "."), unsafe_allow_html=True)


    # --- TABELA DE DADOS ---
    st.markdown("---")
    
    colunas_base = [
        'Número', 'Link Salesforce', 'Abertura', 'Fechamento', 'Origem (Fila Anterior)', 'Quem Aceitou', 'Quem Fechou', 'Fila Principal', 'Subfila', 
        'Qtd Interações (E-mails)', 'Última Interação', 'SLA Estipulado', 'SLA_Dinâmico', 'Conta'
    ]
    colunas_existentes = [c for c in colunas_base if c in df_filtrado.columns]
    df_tabela = df_filtrado[colunas_existentes].copy()

    nomes_colunas = {
        'Número': 'Caso', 
        'Link Salesforce': 'SalesForce', 
        'Origem (Fila Anterior)': 'Veio de', 
        'Fila Principal': 'Fila',
        'Qtd Interações (E-mails)': 'Qtd de Interações', 
        'Conta': 'Cliente'
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


    # --- GRÁFICOS ---
    st.markdown("---")
    graf_col1, graf_col2 = st.columns([1, 1])

    with graf_col1:
        st.markdown("**📊 Visão Geral do SLA**")
        if 'SLA_Dinâmico' in df_filtrado.columns and not df_filtrado.empty:
            sla_counts = df_filtrado['SLA_Dinâmico'].value_counts().reset_index()
            sla_counts.columns = ['Status SLA', 'Quantidade']
            
            mapa_cores = {'🔴 Em Atraso': '#e74c3c', '🟢 No Prazo': '#2ecc71', '⚪ Fechado': '#bdc3c7'}
            fig_donut = px.pie(sla_counts, names='Status SLA', values='Quantidade', hole=0.6, color='Status SLA', color_discrete_map=mapa_cores)
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            fig_donut.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False, height=300, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_donut, use_container_width=True)

    with graf_col2:
        st.markdown("**📈 Top 10 Carteiras (Volume)**")
        if 'Subfila' in df_filtrado.columns and not df_filtrado.empty:
            df_filas_validas = df_filtrado[df_filtrado['Subfila'] != '-']
            fila_counts = df_filas_validas['Subfila'].value_counts().reset_index().head(10)
            fila_counts.columns = ['Carteira', 'Volume']
            
            fig_bar = px.bar(fila_counts, x='Volume', y='Carteira', orientation='h', text='Volume', color_discrete_sequence=['#005eb8'])
            fig_bar.update_traces(textposition='outside')
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=0, b=0, l=0, r=0), height=300, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_bar, use_container_width=True)