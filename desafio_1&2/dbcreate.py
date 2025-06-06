import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from connection import DB_HOST, DB_PASSWORD, DB_PORT, DB_USER, DB_NAME

homologacao_db = "dia1"

def criar_banco_homologacao(db_host, db_senha, db_porta, usuario, db_nome):
    conexao = None
    try:
        conexao = psycopg2.connect(
            host=db_host,
            database=db_nome,
            user=usuario,
            password=db_senha,
            port=db_porta
        )
        print(f"Etapa 1: Conexão Realizada ao DB '{db_nome}'")
        return conexao 
    except OperationalError as e:
        print(f"Erro 1 - Conexão falha: {e} ao tentar conectar ao {db_nome}")
        return None 

def checar_criar_db(alvo_db_name, db_user, db_password, db_host, db_port):
    conexao_padrao = None
    cursor = None 
    try:
        conexao_padrao = criar_banco_homologacao(db_host, db_password, db_port, db_user, DB_NAME)
        if not conexao_padrao:
            print(f"Erro 2 - Erro ao conectar '{DB_NAME}'. Verifique as credenciais.")
            return False

        conexao_padrao.autocommit = True
        cursor = conexao_padrao.cursor()

        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{alvo_db_name}'")
        existe_db_dia1 = cursor.fetchone()

        if not existe_db_dia1:
            print(f"Etapa 2.1 - Banco de dados '{alvo_db_name}' não encontrado. Criando...")
            cursor.execute(f"CREATE DATABASE {alvo_db_name}")
            print(f"Etapa 2.2 - Banco de dados '{alvo_db_name}' criado com sucesso!")
            return True
        else:
            print(f"Etapa 2.3 - Banco de dados '{alvo_db_name}' já existe.")
            return True

    except OperationalError as e:
        print(f"Erro 3.1 - operacional: {e}")
        if e.pgcode == errorcodes.INSUFFICIENT_PRIVILEGE:
            print("Erro 3.2 - Erro de permissão insuficiente. Verifique suas permissões.")
        return False
    except Exception as e:
        print(f"Erro 3.3 - Ocorreu um erro inesperado: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conexao_padrao:
            conexao_padrao.close()
            print(f"Default 1 - Conexão ao banco de dados padrão '{DB_NAME}' fechada.")

def main():
    print(f"Etapa 3.1 - Iniciando verificação e criação do banco de dados '{homologacao_db}'...")
    success = checar_criar_db(homologacao_db, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

    if success:
        print(f"\nEtapa 3.2 - Tentando conexão com '{homologacao_db}'. Aguarde...")
        conn_dia1 = criar_banco_homologacao(DB_HOST, DB_PASSWORD, DB_PORT, DB_USER, homologacao_db)
        if conn_dia1:
            print(f"Etapa 3.2.1 - Conexão de validação ao '{homologacao_db}' bem-sucedida.")
            conn_dia1.close()
            print(f"Etapa 3.2.2 - Conexão de validação ao '{homologacao_db}' fechada.")
        else:
            print(f"Erro 4.1 - Não foi possível estabelecer conexão de validação com '{homologacao_db}'.")
    else:
        print(f"Erro 4.2 - Operação de criação/verificação do banco de dados '{homologacao_db}' falhou.")

if __name__ == "__main__":
    main()