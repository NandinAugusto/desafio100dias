import psycopg2 
from psycopg2 import OperationalError, errorcodes, errors
from connection import DB_HOST, DB_PASSWORD,DB_PORT, DB_USER, DB_PORT

homologacao_db = "dia1" 
def criar_banco_homologacao(db_host, db_password, db_port, db_user, db_name):
    try:
        connection = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Etapa 1: Conexão Realizada")
    except OperationalError as e:
        print(f"Erro de conexão {e}  ao tentar conectar  ao {db_name}")
        return connection