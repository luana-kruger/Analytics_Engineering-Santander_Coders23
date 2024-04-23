import modules.database_connection as db
import pandas as pd

print("Teste")

db.engine_db()

db.criar_database()

db.criar_schemas()


landing_path = "./data/landing/"
file_calendar = landing_path + 'calendar.csv'
df_calendar = pd.read_csv(file_calendar)

'''db.criar_tabela_df('bronze','calendar',df_calendar, 'replace')



file_reviews = landing_path + 'reviews.csv'
df_reviews = pd.read_csv(file_reviews)
db.criar_tabela_df('bronze', 'reviews', df_reviews,'replace')



file_listing = landing_path + 'listings.csv'
df_listings = pd.read_csv(file_listing)
db.criar_tabela_df('bronze', 'listings', df_listings,'replace')'''