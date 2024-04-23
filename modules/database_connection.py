from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateSchema
import pandas as pd
import os


DB_NAME = os.environ.get("DB_NAME")
USER = os.environ.get("USER")
PASSWD = os.environ.get("PASSWD")
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")

def engine_db():
    engine = create_engine(f'postgresql://{USER}:{PASSWD}@{HOST}:{PORT}/{DB_NAME}')
    return engine

def connnection_db():
    engine = engine_db()
    return engine.connect()
    

def criar_database():
    try:
        engine = create_engine(f'postgresql://{USER}:{PASSWD}@{HOST}:{PORT}/postgres')

        # Verifica se o banco de dados "Datalake" já existe
        database_exists = engine.execute(f"SELECT 1 FROM pg_database WHERE datname = lower('{DB_NAME}')").fetchone()
        
        if not database_exists:

            # o Postgres não aceita o comando CREATE DATABASE por transações e o sqlAlchemy sempre executa 
            # as queries em transações. Então é preciso criar uma conexão com o gerenciador.
            # Porem o connect está dentro de uma transação, então depois de fazer o engine connect
            # faremos o 'commit' pra encerrar essa transação e depois fazer o execute com o Create Database
            # e depois fechar a conexão
            engine = create_engine(f'postgresql://{USER}:{PASSWD}@{HOST}:{PORT}/postgres')
            conn = engine.connect()
            conn.execute("commit")
            conn.execute(f"CREATE DATABASE {DB_NAME}")

            conn.close()
            print(f"Banco de dados {DB_NAME} criado com sucesso.")
        else:
            print(f"O banco de dados {DB_NAME} já existe.")
    except OperationalError as error:
        print("Erro de conexão:", error)
    except Exception as e:
        print("Ocorreu um erro:", error)


def criar_schemas():
    try:
        engine = engine_db()
        conn = engine.connect()
    
        exists_bronze = conn.execute(sql_text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze'")).fetchone()
        if not exists_bronze:
            conn.execute(sql_text("CREATE SCHEMA bronze"))
   
            print("Schema 'bronze' criado com sucesso.")
        else:
            print("O Schema 'bronze' ja existe.")
        
        exists_silver = conn.execute(sql_text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'silver'")).fetchone()
        if not exists_silver:
            conn.execute(sql_text("CREATE SCHEMA silver"))

            print("Schema 'silver' criado com sucesso.")
        else:
            print("O Schema 'silver' ja existe.")
    
        exists_gold = conn.execute(sql_text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold'")).fetchone()
        if not exists_gold:
            conn.execute(sql_text("CREATE SCHEMA gold"))

            print("Schema 'gold' criado com sucesso.")
        else:
            print("O Schema 'gold' ja existe.")

        # Fechar a conexão após a conclusão de todas as operações
        conn.commit()    

    except OperationalError as error:
        print("Erro de conexão:", error)
    except Exception as error:
        print("Ocorreu um erro:", error)

def criar_tabela_df(schema:str, nome_tabela:str, df:pd.DataFrame, if_exists:str='append'):
    try:        
        engine = engine_db()
        
        print(f"Criando a tabela '{nome_tabela}' no schema '{schema}'....")
        
        df.to_sql(nome_tabela, engine, schema=schema, if_exists=if_exists, index=False) 
        
        print(f"A tabela '{nome_tabela}' foi criada no schema '{schema}', e os dados foram inseridos.")
        
    except Exception as error:         
        print("Ocorreu um erro:", error)
