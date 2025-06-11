
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import sys

def configurar_logging(nivel_log: str = "INFO", arquivo_log: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger('ETL_Pipeline')
    logger.setLevel(getattr(logging, nivel_log.upper()))
    
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [ETL] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if arquivo_log:
        try:
            Path(arquivo_log).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(arquivo_log, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Não foi possível criar arquivo de log {arquivo_log}: {e}")
    
    return logger

class ConfiguracaoETL:
    
    def __init__(self):
        self.logger = configurar_logging()
        self._carregar_credenciais_db()
        self._configurar_parametros()
    
    def _carregar_credenciais_db(self) -> None:
        try:
            from connection import (
                DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
            )
            self.bd_host = DB_HOST
            self.bd_nome = DB_NAME
            self.bd_usuario = DB_USER
            self.bd_senha = DB_PASSWORD
            self.bd_porta = DB_PORT
            self.logger.info("Credenciais do banco carregadas do módulo connection")
            
        except ImportError as e:
            self.logger.warning(f"Não foi possível importar credenciais de 'connection.py': {e}")
            self.logger.info("Usando credenciais padrão (desenvolvimento)")
            
            self.bd_host = os.getenv('DB_HOST', 'localhost')
            self.bd_nome = os.getenv('DB_NAME', 'postgres')
            self.bd_usuario = os.getenv('DB_USER', 'postgres')
            self.bd_senha = os.getenv('DB_PASSWORD', '123')
            self.bd_porta = os.getenv('DB_PORT', '5432')
    
    def _configurar_parametros(self) -> None:
        """Configura parâmetros do ETL"""
        self.caminho_csv = os.getenv('CAMINHO_CSV', '../desafio100dias/arquivos_diversos/ai_job_dataset.csv')
        self.nome_tabela = os.getenv('NOME_TABELA', 'ai_jobs')
        self.encoding_csv = os.getenv('ENCODING_CSV', 'utf-8')

class ExtractorCSV:
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def extrair_dados(self, caminho_csv: str, encoding: str = 'utf-8') -> Optional[pd.DataFrame]:

        self.logger.info(f"Iniciando extração de dados: {caminho_csv}")
        
        try:
            if not Path(caminho_csv).exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {caminho_csv}")
            
            encodings_tentar = [encoding, 'utf-8', 'latin1', 'cp1252']
            
            for enc in encodings_tentar:
                try:
                    df = pd.read_csv(caminho_csv, encoding=enc)
                    self.logger.info(f"Dados extraídos com sucesso usando encoding '{enc}': "
                                   f"{df.shape[0]} linhas, {df.shape[1]} colunas")
                    return df
                except UnicodeDecodeError:
                    if enc == encodings_tentar[-1]:
                        raise
                    continue
            
        except FileNotFoundError as e:
            self.logger.error(f"Arquivo CSV não encontrado: {e}")
        except pd.errors.EmptyDataError:
            self.logger.error("Arquivo CSV está vazio")
        except pd.errors.ParserError as e:
            self.logger.error(f"Erro ao parsear CSV: {e}")
        except Exception as e:
            self.logger.error(f"Erro inesperado na extração: {e}")
        
        return None

class TransformadorDados:
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def transformar_dados(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:

        if df is None or df.empty:
            self.logger.warning("DataFrame de entrada é nulo ou vazio")
            return None
        
        self.logger.info("Iniciando transformação de dados")
        
        try:
            df_transformado = df.copy()
            
            df_transformado = self._tratar_valores_nulos(df_transformado)
            
            df_transformado = self._remover_duplicatas(df_transformado)
            
            df_transformado = self._padronizar_dados(df_transformado)
            
            df_transformado = self._validar_dados_finais(df_transformado)
            
            self.logger.info(f"Transformação concluída: {df_transformado.shape[0]} linhas, "
                           f"{df_transformado.shape[1]} colunas")
            return df_transformado
            
        except Exception as e:
            self.logger.error(f"Erro durante transformação: {e}")
            return None
    
    def _tratar_valores_nulos(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Tratando valores nulos")
        
        nulos_inicial = df.isnull().sum()
        colunas_com_nulos = nulos_inicial[nulos_inicial > 0]
        
        if len(colunas_com_nulos) > 0:
            self.logger.info(f"Colunas com valores nulos: {dict(colunas_com_nulos)}")
        
        if 'salary_in_usd' in df.columns:
            df['salary_in_usd'] = pd.to_numeric(df['salary_in_usd'], errors='coerce')
            if df['salary_in_usd'].isnull().any():
                mediana = df['salary_in_usd'].median()
                if not pd.isna(mediana):
                    df['salary_in_usd'].fillna(mediana, inplace=True)
                    self.logger.info(f"Valores nulos em salary_in_usd preenchidos com mediana: {mediana}")
        
        colunas_categoricas = ['employment_type', 'company_location', 'job_title', 
                              'experience_level', 'company_size', 'company_residence', 
                              'work_setting']
        
        for col in colunas_categoricas:
            if col in df.columns and df[col].isnull().any():
                try:
                    moda = df[col].mode()[0]
                    df[col].fillna(moda, inplace=True)
                    self.logger.info(f"Valores nulos em {col} preenchidos com moda: {moda}")
                except (IndexError, KeyError):
                    self.logger.warning(f"Não foi possível calcular moda para {col}")
        
        linhas_antes = len(df)
        df.dropna(inplace=True)
        linhas_removidas = linhas_antes - len(df)
        
        if linhas_removidas > 0:
            self.logger.info(f"Removidas {linhas_removidas} linhas com valores nulos restantes")
        
        return df
    
    def _remover_duplicatas(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Removendo duplicatas")
        
        linhas_antes = len(df)
        df.drop_duplicates(inplace=True)
        duplicatas_removidas = linhas_antes - len(df)
        
        if duplicatas_removidas > 0:
            self.logger.info(f"Removidas {duplicatas_removidas} linhas duplicadas")
        else:
            self.logger.info("Nenhuma duplicata encontrada")
        
        return df
    
    def _padronizar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Padronizando dados")
        
        colunas_texto = ['job_title', 'experience_level', 'employment_type', 
                        'company_location', 'company_size', 'company_residence', 
                        'work_setting']
        
        for col in colunas_texto:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.lower().str.strip()
                self.logger.debug(f"Coluna {col} padronizada")
        
        return df
    
    def _validar_dados_finais(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Validando dados finais")
        
        nulos_finais = df.isnull().sum().sum()
        if nulos_finais > 0:
            self.logger.warning(f"Ainda existem {nulos_finais} valores nulos após transformação")
        
        if df.empty:
            self.logger.error("DataFrame está vazio após transformações")
            return None
        
        self.logger.info("Validação de dados concluída com sucesso")
        return df

class ConectorBancoDados:
    
    def __init__(self, config: ConfiguracaoETL):
        self.config = config
        self.logger = config.logger
        self.engine = None
    
    def conectar(self) -> bool:

        self.logger.info("Conectando ao banco de dados PostgreSQL")
        
        try:
            string_conexao = (f'postgresql+psycopg2://{self.config.bd_usuario}:'
                            f'{self.config.bd_senha}@{self.config.bd_host}:'
                            f'{self.config.bd_porta}/{self.config.bd_nome}')
            
            self.engine = create_engine(string_conexao, pool_pre_ping=True)
            
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("Conexão com banco de dados estabelecida com sucesso")
            return True
            
        except Exception as e:
            self.logger.error(f"Falha na conexão com banco de dados: {e}")
            return False
    
    def carregar_dados(self, df: pd.DataFrame, nome_tabela: str) -> bool:
     
        if df is None or df.empty:
            self.logger.warning("DataFrame vazio, nenhum dado será carregado")
            return False
        
        if self.engine is None:
            self.logger.error("Conexão com banco não estabelecida")
            return False
        
        self.logger.info(f"Carregando {len(df)} registros na tabela '{nome_tabela}'")
        
        try:
            df.to_sql(nome_tabela, self.engine, if_exists='replace', index=False)
            self.logger.info(f"Dados carregados com sucesso na tabela '{nome_tabela}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados: {e}")
            return False
    
    def desconectar(self) -> None:
        if self.engine:
            self.engine.dispose()
            self.logger.info("Conexão com banco de dados fechada")

class PipelineETL:
    
    def __init__(self):
        self.config = ConfiguracaoETL()
        self.logger = self.config.logger
        self.extractor = ExtractorCSV(self.logger)
        self.transformador = TransformadorDados(self.logger)
        self.conector_bd = ConectorBancoDados(self.config)
    
    def executar(self) -> bool:

        self.logger.info("="*60)
        self.logger.info("INICIANDO PROCESSO ETL")
        self.logger.info("="*60)
        
        sucesso = False
        
        try:
            self.logger.info("ETAPA 1: EXTRAÇÃO DE DADOS")
            df_dados = self.extractor.extrair_dados(
                self.config.caminho_csv, 
                self.config.encoding_csv
            )
            
            if df_dados is None:
                self.logger.error("Falha na extração de dados")
                return False
            
            self.logger.info("ETAPA 2: TRANSFORMAÇÃO DE DADOS")
            df_transformado = self.transformador.transformar_dados(df_dados)
            
            if df_transformado is None:
                self.logger.error("Falha na transformação de dados")
                return False
            
            self.logger.info("ETAPA 3: CARGA DE DADOS")
            if not self.conector_bd.conectar():
                self.logger.error("Falha na conexão com banco de dados")
                return False
            
            sucesso = self.conector_bd.carregar_dados(df_transformado, self.config.nome_tabela)
            
        except Exception as e:
            self.logger.error(f"Erro inesperado no pipeline ETL: {e}")
            
        finally:
            self.conector_bd.desconectar()
            
            if sucesso:
                self.logger.info("="*60)
                self.logger.info("PROCESSO ETL CONCLUÍDO COM SUCESSO")
                self.logger.info("="*60)
            else:
                self.logger.error("="*60)
                self.logger.error("PROCESSO ETL FALHOU")
                self.logger.error("="*60)
        
        return sucesso

def main():
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo_log = f"logs/etl_{timestamp}.log"
        
        Path("logs").mkdir(exist_ok=True)
        
        pipeline = PipelineETL()
        sucesso = pipeline.executar()
        
        sys.exit(0 if sucesso else 1)
        
    except KeyboardInterrupt:
        print("\nProcesso interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"Erro crítico: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
