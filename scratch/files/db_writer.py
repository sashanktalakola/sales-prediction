from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

df = pd.read_csv("./full_data_csv/part-00000-5a21b66d-3889-48b7-8047-b3ebb15aeefd-c000.csv")
print(df.head())

db_url = f"postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_IP")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}"

engine = create_engine(db_url)
df.to_sql('Sales', engine, index=False, if_exists='replace')


from sqlalchemy.orm import Session

with Session(engine) as session:
    # Convert DataFrame to list of dictionaries
    records = df.to_dict('records')
    
    # Bulk insert
    session.execute(
        table.insert(), 
        records
    )
    session.commit()