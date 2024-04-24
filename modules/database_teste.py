import os
import pandas as pd
from typing import Literal
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL, text


class Database:
    """
    Esta classe gerencia conexões a um banco de dados PostgreSQL e fornece métodos para criar bancos de dados, schemas e inserir DataFrames.

    Atributos:
        usuario (str): O nome de usuário para se conectar ao banco de dados. (Obtido da variável de ambiente)
        senha (str): A senha para se conectar ao banco de dados. (Obtida da variável de ambiente)
        host (str): O hostname ou endereço IP do servidor do banco de dados. (Obtido da variável de ambiente)
        porta (str): O número da porta do servidor do banco de dados. (Obtido da variável de ambiente)
    """

    def __init__(self) -> None:
        """
        Inicializa a classe 'Database' carregando variáveis de ambiente para credenciais do banco de dados 
        e armazenando-as nos atributos do objeto.

        Raises:
            RuntimeError: Se alguma das variáveis de ambiente necessárias (USER, PASSWD, HOST, PORT) não for encontrada.
        """
        load_dotenv()
        self.user = os.getenv("USER")
        self.password = os.getenv("PASSWD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")

    def create_connection(self,database:str=None):
        """
        Estabelece uma conexão com o banco de dados PostgreSQL.

        Args:
            banco_de_dados (str, optional): O nome do banco de dados específico para se conectar. Padrão para None (conecta ao banco de dados padrão).

        Returns:
            Connection: Um objeto SQLAlchemy Engine representando a conexão com o banco de dados.
        """
        url_object = URL.create(
            "postgresql",
            username=self.user,
            password=self.password,
            host=self.host,
            database=database,
            port=self.port
        )
        return create_engine(url=url_object,echo=False,isolation_level="AUTOCOMMIT").connect()
    
    def create_database(self, database:str):
        """
        Cria um novo banco de dados PostgreSQL.

        Args:
            banco_de_dados (str): O nome do banco de dados a ser criado.

        Raise:
            Exception: Se ocorrer um erro inesperado durante a criação do banco de dados.
        """

        with self.create_connection() as connection:
            query_text = f'''
            CREATE DATABASE {database}
                WITH
                OWNER = postgres
                ENCODING = 'UTF8'
                LOCALE_PROVIDER = 'libc'
                CONNECTION LIMIT = -1
                IS_TEMPLATE = False;
            '''
            query_obj = text(query_text)
            connection.execute(query_obj)

    def create_schema(self,schema_name:str,database:str=None):
        """
        Cria um novo schema dentro de um banco de dados PostgreSQL.

        Args:
            nome_do_esquema (str): O nome do schema a ser criado.
            banco_de_dados (str, optional): O nome do banco de dados onde o schema será criado. Padrão para None (cria o schema no banco de dados padrão).

        Raise:
            Exception: Se ocorrer um erro inesperado durante a criação do schema.
        """
        try:
            with self.create_connection(database=database) as connection:
                query_text = f"CREATE SCHEMA {schema_name} AUTHORIZATION {self.user};"
                query_obj = text(query_text)
                connection.execute(query_obj)
        except Exception as e:
            print(f"Um erro inesperado ocorreu ao criar o schema '{schema_name}': {e}")
    
    def insert_dataframe(self,dataframe:pd.DataFrame,table_name:str,schema:str,database:str=None,if_exists:Literal["fail", "replace", "append"] = "append"):
        """
        Insere um DataFrame do Pandas em uma tabela PostgreSQL.

        Args:
            dataframe (pandas.DataFrame): O DataFrame a ser inserido.
            nome_da_tabela (str): O nome da tabela para inserir os dados.
            esquema (str): O nome do schema onde a tabela reside.
            banco_de_dados (str, optional): O nome do banco de dados onde a tabela está localizada. Padrão para None (usa o banco de dados padrão).
            if_exists (Literal["fail", "replace", "append"], optional): Como lidar com uma tabela existente. Padrão para "append" (adiciona dados).
                * "fail": falha se a tabela já existir.
                * "replace": exclui a tabela existente e insere o DataFrame.
                * "append": adiciona os dados do DataFrame à tabela existente.

        Levanta:
            Exception: Se ocorrer um erro inesperado durante a inserção do DataFrame.
        """
        
        with self.create_connection(database=database) as connection:
            try:
                print(f"Criando a tabela '{table_name}' na base de dados '{database}' no schema {schema}.")
                dataframe.to_sql(name=table_name,schema=schema,con=connection,if_exists=if_exists)
                print("O dataframe inserido com sucesso!")
            except Exception as e:
                print(f"Erro ao inserir o dataframe na tabela '{table_name}': {e}")
                raise e