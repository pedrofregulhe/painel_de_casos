import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime, timedelta, timezone
import time
import subprocess
import os

# --- CONFIGURAÇÕES E CREDENCIAIS ---
SF_USER = "ext-potavio@culligan.com"
SF_PWD = "Bankai@Toshiro1025" 
SF_TOKEN = "Focq5VJHTLn6TI5ZFpJCB3ZF7" 

CAMPO_ITEM_CONTRATO = 'FOZ_Asset__r.FOZ_CodigoItem__c'
ARQUIVO_SAIDA = 'Base_OA_PowerBI.csv'
ARQUIVO_DATA_HORA = 'data_hora_atualização.txt'
NOME_DO_BAT = 'atualizar_github.bat'

def extract_field(record, field_path):
    parts = field_path.split('.')
    val = record
    for part in parts:
        if val and isinstance(val, dict):
            val = val.get(part)
        else:
            return ""
    return str(val) if val is not None else ""

def carregar_basecorp():
    basecorp_dict = {}
    try:
        df_bc = pd.read_excel('basecorp.xlsx')
        df_bc.columns = df_bc.columns.str.lower().str.strip()
        df_bc['itemcontrato'] = df_bc['itemcontrato'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.lstrip('0')
        basecorp_dict = dict(zip(df_bc['itemcontrato'], df_bc['carteira'].astype(str).str.strip()))
    except:
        pass
    return basecorp_dict

# --- NOVA FUNÇÃO PARA AJUSTAR O FUSO HORÁRIO (-3 HORAS) ---
def ajustar_fuso(data_string):
    if not data_string:
        return None
    # Converte o UTC do Salesforce para Datetime e remove 3 horas para o horário de Brasília
    return pd.to_datetime(data_string).tz_localize(None) - timedelta(hours=3)

def extrair_e_processar():
    try:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🚀 Iniciando extração (ajustando fuso horário para o Brasil)...")
        sf = Salesforce(username=SF_USER, password=SF_PWD, security_token=SF_TOKEN, domain='login')
        basecorp_dict = carregar_basecorp()
        
        query = f"""
        SELECT 
            Id, CaseNumber, CreatedDate, ClosedDate, Status, Description, Origin, Type, 
            FOZ_TipoSolicitacao__c, FOZ_Motivo__c, FOZ_Detalhe__c, FOZ_SubStatus__c, OwnerId, Owner.Name, 
            Account.Name, Account.FOZ_CNPJ__c, {CAMPO_ITEM_CONTRATO},
            (SELECT IsViolated, TargetDate, MilestoneType.Name FROM CaseMilestones ORDER BY TargetDate ASC),
            (SELECT Id, MessageDate FROM EmailMessages),
            (SELECT Id, CreatedDate FROM CaseComments)
        FROM Case 
        WHERE Type != 'OS' 
          AND (Type = 'OA' OR Owner.Name LIKE 'CARTEIRA%' OR Owner.Name LIKE '%GENÉRICO%' OR Owner.Name LIKE '%SEM FILA%')
          AND CreatedDate = LAST_N_DAYS:180
        """
        
        result = sf.query(query)
        records = result.get('records', [])
        while not result.get('done', True):
            result = sf.query_more(result['nextRecordsUrl'], True)
            records.extend(result.get('records', []))

        linhas = []
        agora_br = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=3)
        sf_base_url = "https://ibbl.lightning.force.com/lightning/r/Case/"
        
        for record in records:
            status_real_sf = str(record.get('Status') or '').strip().lower()
            dono_upper = str(record['Owner']['Name'] or '').upper() if record.get('Owner') else 'SISTEMA/SEM DONO'
            filas_conhecidas = ["ERRO SISTÊMICO", "CAPACIDADE", "FRANQUIAS", "AUDITORIA", "HELP TEC", "JURÍDICO", "INFORMAÇÃO", "RAF", "FINANCEIRO", "BACKOFFICE"]
            
            if "SAFETY" in dono_upper: fila_principal, subfila = "SAFETY", dono_upper
            elif "GENÉRICO" in dono_upper or "SEM FILA" in dono_upper: fila_principal, subfila = "CASOS SEM FILA - GENÉRICO", dono_upper
            elif dono_upper in filas_conhecidas: fila_principal, subfila = dono_upper, "-"
            elif dono_upper.startswith("CARTEIRA"): fila_principal, subfila = "CORPORATIVO", dono_upper
            else: fila_principal, subfila = "ATRIBUÍDO AO USUÁRIO", dono_upper
                
            macro_status = "Fechado" if record.get('Status') in ['Closed', 'Fechado'] else "Em Tratativa"
            
            # --- APLICANDO O AJUSTE DE FUSO NAS DATAS PRINCIPAIS ---
            data_abertura = ajustar_fuso(record.get('CreatedDate')) or agora_br
            data_fechamento = ajustar_fuso(record.get('ClosedDate'))
            
            datas_interacoes = [data_abertura] 
            emails = record.get('EmailMessages')
            if emails and 'records' in emails:
                for em in emails['records']:
                    if em.get('MessageDate'):
                        datas_interacoes.append(ajustar_fuso(em['MessageDate']))
            comentarios = record.get('CaseComments')
            if comentarios and 'records' in comentarios:
                for cc in comentarios['records']:
                    if cc.get('CreatedDate'):
                        datas_interacoes.append(ajustar_fuso(cc['CreatedDate']))
            ultima_interacao = max(datas_interacoes)

            sla_macro = "No Prazo"
            regra_sla_sf = "Sem SLA SF"
            if record.get('CaseMilestones') and record['CaseMilestones'].get('records'):
                milestones = record['CaseMilestones']['records']
                mt = milestones[0].get('MilestoneType')
                if mt and isinstance(mt, dict):
                    regra_sla_sf = str(mt.get('Name', 'Sem SLA SF'))
                if any(m.get('IsViolated') for m in milestones):
                    sla_macro = "Atrasado"

            if fila_principal == "CASOS SEM FILA - GENÉRICO" and status_real_sf in ["aberto", "em aberto"]:
                if (ultima_interacao + timedelta(hours=24) - agora_br).total_seconds() < 0:
                    sla_macro = "Atrasado"
            elif fila_principal == "CORPORATIVO" and status_real_sf in ["aberto", "em aberto"]:
                if (ultima_interacao + timedelta(hours=48) - agora_br).total_seconds() < 0:
                    sla_macro = "Atrasado"

            idade_dias = ((data_fechamento if data_fechamento else agora_br) - data_abertura).days
            acc = record.get('Account') or {}
            qtd_emails = len(emails['records']) if emails and 'records' in emails else 0
            qtd_comentarios = len(comentarios['records']) if comentarios and 'records' in comentarios else 0
            
            raw_item_contrato = str(extract_field(record, CAMPO_ITEM_CONTRATO) or '').strip()
            item_contrato_limpo = raw_item_contrato.lstrip('0') if raw_item_contrato else "0"
            carteira_basecorp = str(basecorp_dict.get(item_contrato_limpo, "-") or "-")
            
            linhas.append({
                'Número': record.get('CaseNumber'),
                'Link Salesforce': f"{sf_base_url}{record.get('Id')}/view",
                'Fila Principal': fila_principal,
                'Subfila': subfila,
                'Conta': str(acc.get('Name') or '-'),
                'CNPJ': str(acc.get('FOZ_CNPJ__c') or '-'),
                'Abertura': data_abertura.strftime('%d/%m/%Y %H:%M:%S'),
                'Fechamento': data_fechamento.strftime('%d/%m/%Y %H:%M:%S') if data_fechamento else "",
                'Status': str(record.get('Status') or ''),
                'Macro Status': macro_status,
                'SLA Macro': sla_macro,
                'Regra SLA SF': regra_sla_sf,
                'Idade (Dias)': idade_dias,
                'BaseCorp Carteira': carteira_basecorp,
                'Qtd Interações (E-mails)': qtd_emails + qtd_comentarios,
                'Última Interação': ultima_interacao.strftime('%d/%m/%Y %H:%M:%S')
            })
            
        df_final = pd.DataFrame(linhas)
        df_final.to_csv(ARQUIVO_SAIDA, index=False, encoding='utf-8-sig')
        
        agora_txt = datetime.now().strftime('%d/%m/%Y às %H:%M')
        with open(ARQUIVO_DATA_HORA, "w", encoding="utf-8") as f:
            f.write(agora_txt)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Base salva e arquivo de data atualizado.")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Enviando para o GitHub via {NOME_DO_BAT}...")
        subprocess.run([NOME_DO_BAT], shell=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ciclo finalizado com sucesso!")

    except Exception as e:
        print(f"❌ Erro durante o ciclo: {e}")

if __name__ == "__main__":
    while True:
        extrair_e_processar()
        
        proxima_rodada = datetime.now() + timedelta(hours=1)
        print(f"💤 Dormindo... Próxima atualização será às: {proxima_rodada.strftime('%H:%M:%S')}")
        
        time.sleep(3600)