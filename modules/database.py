from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.environ["DB_NAME"]
USER = os.environ["USER"]
PASSWD = os.environ["PASSWD"]
HOST = os.environ["HOST"]
PORT = os.environ["PORT"]


def engine_db(database:str=None):
    '''
    '''
    connection_string = f'postgresql://{USER}:{PASSWD}@{HOST}:{PORT}'
    
    if database is not None:
        connection_string+=f'/{database}'
    
    return create_engine(connection_string)

def connnection_db():
    engine = engine_db()
    return engine.connect()
    

def criar_database(database:str):
    try:
        engine = create_engine(f'postgresql://{USER}:{PASSWD}@{HOST}:{PORT}')

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:

            # Verifica se o banco de dados já existe
            sql = f"SELECT 1 FROM pg_database WHERE datname = lower('{database}')"
            database_exists = conn.execute(text(sql)).fetchone()

            if not database_exists:
                sql_create_db = f"CREATE DATABASE {database}"
                conn.execute(text(sql_create_db))
                conn.commit()

                conn.close()
                print(f"Banco de dados {database} criado com sucesso.")
            else:
                print(f"O banco de dados {database} já existe.")
    except OperationalError as error:
        print("Erro de conexão:", error)
        raise error
    except Exception as error:
        print("Ocorreu um erro:", error)
        raise error


def criar_schemas(database:str):
    try:
        engine = engine_db(database=database)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            sql_exists_bronze = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze'"
            exists_bronze = conn.execute(text(sql_exists_bronze)).fetchone()
            if not exists_bronze:
                sql_create_bronze = "CREATE SCHEMA bronze"
                conn.execute(text(sql_create_bronze))
                #conn.commit()
                print("Schema 'bronze' criado com sucesso.")
            else:
                print("O Schema 'bronze' ja existe.")


            sql_exists_silver = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'silver'"
            exists_silver = conn.execute(text(sql_exists_silver)).fetchone()
            if not exists_silver:
                sql_create_silver = "CREATE SCHEMA silver"
                conn.execute(text(sql_create_silver))
                # conn.commit()
                print("Schema 'silver' criado com sucesso.")
            else:
                print("O Schema 'silver' ja existe.")

            sql_exists_gold = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold'"
            exists_gold = conn.execute(text(sql_exists_gold)).fetchone()
            if not exists_gold:
                sql_create_gold = "CREATE SCHEMA gold"
                conn.execute(text(sql_create_gold))
                # conn.commit()
                print("Schema 'gold' criado com sucesso.")
            else:
                print("O Schema 'gold' ja existe.")

    except OperationalError as error:
        print("Erro de conexão:", error)
        raise error
    except Exception as error:
        print("Ocorreu um erro:", error)
        raise error

def criar_tabela_df(database:str,schema:str, nome_tabela:str, df:pd.DataFrame, if_exists:str='append'):
    try:        
        engine = engine_db(database=database)
        
        print(f"Criando a tabela '{nome_tabela}' no schema '{schema}'....")
        
        df.to_sql(nome_tabela, engine, schema=schema, if_exists=if_exists, index=False) 
        
        print(f"A tabela '{nome_tabela}' foi criada no schema '{schema}', e os dados foram inseridos.")
        
    except Exception as error:
        raise error
