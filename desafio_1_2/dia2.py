import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
import os
import datetime

try:
    from connection import (
        DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
    )
    BD_HOST = DB_HOST
    BD_NOME = DB_NAME
    BD_USUARIO = DB_USER
    BD_SENHA = DB_PASSWORD
    BD_PORTA = DB_PORT
    
except ImportError as e:
    print(f"ERRO CRÍTICO: Não foi possível importar as credenciais de 'connection.py'.")
    print(f"Verifique se o caminho da importação está correto e se as pastas são pacotes Python (com __init__.py).")
    print(f"Erro: {e}")
    BD_HOST = "localhost"
    BD_NOME = "postgres"
    BD_USUARIO = "postgres"
    BD_SENHA = "123"
    BD_PORTA = "5432"

NUMERO_ETAPA = 0

def registrar_mensagem(mensagem, nivel="INFO"):
    global NUMERO_ETAPA
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if nivel == "INFO" or nivel == "SUCCESS" or nivel == "INICIO" or nivel == "FIM":
        NUMERO_ETAPA += 1
        print(f"[{timestamp}] [{nivel}] [ETAPA {NUMERO_ETAPA:.1f}] {mensagem}")
    elif nivel == "ERRO":
        print(f"[{timestamp}] [{nivel}] [ERRO {NUMERO_ETAPA:.1f}] {mensagem}")
    elif nivel == "AVISO":
        print(f"[{timestamp}] [{nivel}] [AVISO {NUMERO_ETAPA:.1f}] {mensagem}")
    elif nivel.startswith("SUB-ETAPA"):
        try:
            sub_etapa_val = float(nivel.split(" ")[-1])
            print(f"[{timestamp}] [INFO] [ETAPA {NUMERO_ETAPA}.{sub_etapa_val:.1f}] {mensagem}")
        except ValueError:
            print(f"[{timestamp}] [INFO] [ETAPA {NUMERO_ETAPA:.1f}] {mensagem}")
    else:
        print(f"[{timestamp}] [{nivel}] {mensagem}")

CAMINHO_ARQUIVO_CSV = '../desafio100dias/arquivos_diversos/ai_job_dataset.csv'
NOME_TABELA = 'ai_jobs'

def conectar_bd_alchemy():
    registrar_mensagem("Tentando conectar ao banco de dados PostgreSQL via SQLAlchemy...", nivel="INFO")
    try:
        string_conexao_engine = f'postgresql+psycopg2://{BD_USUARIO}:{BD_SENHA}@{BD_HOST}:{BD_PORTA}/{BD_NOME}'
        engine = create_engine(string_conexao_engine)
        with engine.connect() as conexao_teste:
            conexao_teste.execute(text("SELECT 1"))
        registrar_mensagem("Conexão com o banco de dados PostgreSQL estabelecida com sucesso.", nivel="SUCCESS")
        return engine
    except Exception as e:
        registrar_mensagem(f"Falha ao conectar ao banco de dados via SQLAlchemy. Verifique suas credenciais e a disponibilidade do DB: {e}", nivel="ERRO")
        return None

def extrair_dados(caminho_csv):
    registrar_mensagem(f"Iniciando a extração de dados do arquivo CSV: {caminho_csv}", nivel="INFO")
    try:
        df = pd.read_csv(caminho_csv)
        registrar_mensagem(f"Dados extraídos com sucesso. {df.shape[0]} linhas, {df.shape[1]} colunas.", nivel="SUCCESS")
        return df
    except FileNotFoundError:
        registrar_mensagem(f"O arquivo CSV '{caminho_csv}' não foi encontrado. Verifique o caminho.", nivel="ERRO")
        return None
    except Exception as e:
        registrar_mensagem(f"Falha ao extrair dados do CSV: {e}", nivel="ERRO")
        return None

def transformar_dados(df):
    if df is None:
        registrar_mensagem("DataFrame de entrada é nulo. Nenhuma transformação será realizada.", nivel="AVISO")
        return None

    registrar_mensagem("Iniciando a Transformação de Dados.", nivel="INFO")

    registrar_mensagem("Tratamento de valores nulos.", nivel="SUB-ETAPA 1.1")
    contagem_nulos_inicial = df.isnull().sum()
    registrar_mensagem(f"Valores nulos antes do tratamento:\n{contagem_nulos_inicial[contagem_nulos_inicial > 0]}", nivel="INFO")

    if 'salary_in_usd' in df.columns:
        registrar_mensagem("Preenchendo valores nulos em 'salary_in_usd' com a mediana.", nivel="SUB-ETAPA 1.2")
        df['salary_in_usd'] = pd.to_numeric(df['salary_in_usd'], errors='coerce')
        mediana_salario = df['salary_in_usd'].median()
        if pd.isna(mediana_salario):
            registrar_mensagem("Mediana de 'salary_in_usd' é NaN (coluna vazia ou não numérica). Não preenchido.", nivel="AVISO")
        else:
            df['salary_in_usd'].fillna(mediana_salario, inplace=True)
            registrar_mensagem(f"Valores nulos em 'salary_in_usd' preenchidos com a mediana ({mediana_salario}).", nivel="INFO")

    colunas_categoricas_para_preencher = ['employment_type', 'company_location', 'job_title', 'experience_level', 'company_size', 'company_residence', 'work_setting']
    for col in colunas_categoricas_para_preencher:
        if col in df.columns:
            if df[col].isnull().any():
                registrar_mensagem(f"Preenchendo valores nulos na coluna categórica '{col}' com a moda.", nivel="SUB-ETAPA 1.3")
                try:
                    valor_moda = df[col].mode()[0]
                    df[col].fillna(valor_moda, inplace=True)
                    registrar_mensagem(f"Valores nulos em '{col}' preenchidos com a moda ('{valor_moda}').", nivel="INFO")
                except IndexError:
                    registrar_mensagem(f"Coluna '{col}' não possui moda válida ou está vazia. Não preenchido.", nivel="AVISO")

    linhas_iniciais_antes_dropna = df.shape[0]
    registrar_mensagem(f"Verificando e removendo linhas com NaNs restantes (antes: {linhas_iniciais_antes_dropna} linhas).", nivel="SUB-ETAPA 1.4")
    df.dropna(inplace=True)
    linhas_apos_dropna = df.shape[0]
    if linhas_iniciais_antes_dropna > linhas_apos_dropna:
        registrar_mensagem(f"Removidas {linhas_iniciais_antes_dropna - linhas_apos_dropna} linhas com valores nulos restantes.", nivel="SUCCESS")
    else:
        registrar_mensagem("Nenhuma linha removida devido a valores nulos restantes após preenchimento.", nivel="INFO")

    contagem_nulos_final = df.isnull().sum()
    if contagem_nulos_final.sum() == 0:
        registrar_mensagem("Todos os valores nulos foram tratados.", nivel="SUCCESS")
    else:
        registrar_mensagem(f"Ainda existem valores nulos após o tratamento:\n{contagem_nulos_final[contagem_nulos_final > 0]}", nivel="AVISO")

    registrar_mensagem("Identificação e remoção de duplicatas.", nivel="SUB-ETAPA 2.1")
    linhas_iniciais_antes_dedupe = df.shape[0]
    df.drop_duplicates(inplace=True)
    linhas_apos_dedupe = df.shape[0]
    if linhas_iniciais_antes_dedupe > linhas_apos_dedupe:
        registrar_mensagem(f"Removidas {linhas_iniciais_antes_dedupe - linhas_apos_dedupe} linhas duplicadas.", nivel="SUCCESS")
    else:
        registrar_mensagem("Nenhuma linha duplicada encontrada.", nivel="INFO")

    registrar_mensagem("Padronização de formatos (minúsculas e remoção de espaços em branco).", nivel="SUB-ETAPA 3.1")
    colunas_string_para_padronizar = ['job_title', 'experience_level', 'employment_type', 'company_location', 'company_size', 'company_residence', 'work_setting']
    for col in colunas_string_para_padronizar:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.lower().str.strip()
            registrar_mensagem(f"Coluna '{col}' padronizada.", nivel="INFO")
    
    registrar_mensagem(f"Transformação de dados concluída. DataFrame final: {df.shape[0]} linhas, {df.shape[1]} colunas.", nivel="SUCCESS")
    return df

def carregar_dados(df, engine, nome_tabela):
    if df is None or engine is None:
        registrar_mensagem("Dados ou conexão com o banco de dados inválidos para carregamento.", nivel="AVISO")
        return False

    registrar_mensagem(f"Iniciando carregamento de dados para a tabela '{nome_tabela}' no PostgreSQL.", nivel="INFO")
    try:
        df.to_sql(nome_tabela, engine, if_exists='replace', index=False)
        registrar_mensagem(f"Dados carregados com sucesso para a tabela '{nome_tabela}'.", nivel="SUCCESS")
        return True
    except Exception as e:
        registrar_mensagem(f"Falha ao carregar dados para o banco de dados: {e}", nivel="ERRO")
        return False

if __name__ == '__main__':
    registrar_mensagem("--- INÍCIO DO PROCESSO ETL ---", nivel="INICIO")

    registrar_mensagem("Extração de Dados.", nivel="INFO")
    dataframe_dados = extrair_dados(CAMINHO_ARQUIVO_CSV)

    if dataframe_dados is not None:
        registrar_mensagem("Transformação de Dados.", nivel="INFO")
        dataframe_dados_limpo = transformar_dados(dataframe_dados)

        if dataframe_dados_limpo is not None:
            registrar_mensagem("Carga de Dados.", nivel="INFO")
            engine_bd = conectar_bd_alchemy()
            if engine_bd:
                carregar_dados(dataframe_dados_limpo, engine_bd, NOME_TABELA)
                engine_bd.dispose()
                registrar_mensagem("Conexões com o banco de dados liberadas.", nivel="INFO")
            else:
                registrar_mensagem("Conexão com o banco de dados falhou, a carga não pôde ser realizada.", nivel="ERRO")
        else:
            registrar_mensagem("A etapa de transformação de dados falhou ou retornou um DataFrame vazio. Carga não realizada.", nivel="ERRO")
    else:
        registrar_mensagem("A etapa de extração de dados falhou. Processo ETL interrompido.", nivel="ERRO")

    registrar_mensagem("--- FIM DO PROCESSO ETL ---", nivel="FIM")