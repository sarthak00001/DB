# import psycopg
# import os
# from dotenv import load_dotenv

# load_dotenv()

# conn = psycopg.connect(
#     host=os.getenv("DB_HOST"),
#     port=os.getenv("DB_PORT"),
#     dbname=os.getenv("DB_NAME"),
#     user=os.getenv("DB_USER"),
#     password=os.getenv("DB_PASSWORD"),
# )

# print("Connected successfully!")
# conn.close()


# import psycopg, os
# conn = psycopg.connect(
#     host=os.getenv("DB_HOST"),
#     port=os.getenv("DB_PORT"),
#     dbname=os.getenv("DB_NAME"),
#     user=os.getenv("DB_USER"),
#     password=os.getenv("DB_PASSWORD"),
# )
# cur = conn.cursor()
# cur.execute("SELECT current_database(), current_user;")
# print(cur.fetchone())


import pandas as pd
df = pd.read_csv("/Users/sarthakchandra/Desktop/Tradewise/DataBase/data/dec rev.csv", nrows=5)
print(df.columns.tolist())

