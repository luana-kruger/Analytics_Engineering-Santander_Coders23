import os
import modules.database as db
from modules.database_teste import Database
import pandas as pd

print("Teste")

#db.engine_db()

#db.criar_database()

#db.criar_schemas()

#landing_path = "./data/landing/"
#file_calendar = landing_path + 'calendar.csv'
#df_calendar = pd.read_csv(file_calendar)

'''db.criar_tabela_df('bronze','calendar',df_calendar, 'replace')



file_reviews = landing_path + 'reviews.csv'
df_reviews = pd.read_csv(file_reviews)
db.criar_tabela_df('bronze', 'reviews', df_reviews,'replace')



file_listing = landing_path + 'listings.csv'
df_listings = pd.read_csv(file_listing)
db.criar_tabela_df('bronze', 'listings', df_listings,'replace')'''

#### Teste Rafael (Bronze)
## Criação de base de dados
database = Database() # Inicialiando a classe com as variaveis de conexão com o banco
database.create_database('datalake') # Criando o database datalake

## Criação dos Schema
database.create_schema(schema_name='bronze',database='datalake')
database.create_schema(schema_name='silver',database='datalake')
database.create_schema(schema_name='gold',database='datalake')

## Inserção dos dataframes
data_path = './data/'
df_listings = pd.read_csv(os.path.join(data_path,'listings.csv'))
database.insert_dataframe(dataframe=df_listings,table_name='listings',schema='bronze',database='datalake',if_exists='append')

df_reviews = pd.read_csv(os.path.join(data_path,'reviews.csv'))
database.insert_dataframe(dataframe=df_reviews,table_name='reviews',schema='bronze',database='datalake',if_exists='append')

df_calendar = pd.read_csv(os.path.join(data_path,'calendar.csv'))
database.insert_dataframe(dataframe=df_calendar,table_name='calendar',schema='bronze',database='datalake',if_exists='append')