import urllib.request
import logging
import psycopg2 
import pandas as pd
import os
from dotenv import load_dotenv
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
load_dotenv()

url = "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv"


host = os.getenv("postgres_host")
db_name = os.getenv('db')
user_name = os.getenv("postgres_user")
user_password = os.getenv("postgres_password")
port = os.getenv("postgres_port")
dest_file=os.getenv("dest_folder")

destination_path=f'{dest_file}/data.csv'

try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=user_name,
        password=user_password,
        host=host,
        port=port
    )
    cur=conn.cursor()
    logging.info("Connection to database was successful")
except Exception as e:
    logging.info(f"Something Went wrong when connecting to database {e}")


def download_file_from_url(url:str, dest_file:str):
    '''
    downloads the file from the web and places it in a file within our directory
    '''
    if not os.path.exists(str(dest_file)):
        os.makedirs(str(dest_file))
    try:
        urllib.request.urlretrieve(url=url,filename=destination_path)
        logging.info("csv file donwloaded correctly to the working directory")
    except Exception as e:
        logging.info(f"Error while downloading file due to {e}")





def create_postgres_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")

        logging.info(' New table churn_modelling created successfully to postgres server')
    except:
        logging.warning(' Check if the table churn_modelling exists')


def write_to_postgres():
    df = pd.read_csv(destination_path,delimiter=',')
    inserted_row_count = 0
    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
        cur.execute(count_query)
        result = cur.fetchone()

        if result[0] == 0:
            inserted_row_count += 1
            cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""",
                    (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))

            logging.info(f' {inserted_row_count} rows from csv file inserted into churn_modelling table successfully')


def main():
    download_file_from_url(url, dest_file)
    create_postgres_table()
    write_to_postgres()
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
#%%
