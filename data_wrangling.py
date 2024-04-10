import logging

import pandas
import pandas as pd
import psycopg2
import numpy as np
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
from dotenv import load_dotenv

load_dotenv()
host = os.getenv("postgres_host")
db_name = os.getenv('db')
user_name = os.getenv("postgres_user")
user_password = os.getenv("postgres_password")
port = os.getenv("postgres_port")

try:
    conn = psycopg2.connect(dbname=db_name,
                            user=user_name,
                            password=user_password,
                            host=host,
                            port=port
                            )

    cur = conn.cursor()
    logging.info("Connection to database was successful")
except Exception as e:
    logging.info(f"Something Went wrong when connecting to database {e}")


def extract_data_from_db():
    '''
    This function extracts the data from the postgres db
    :return: dataframe
    '''

    query = """ SELECT * FROM churn_modelling """
    df = pd.read_sql(query, con=conn)
    df.drop(columns=["rownumber"], inplace=True, axis=1)
    logging.info("Data extraction was successful")
    return df


def create_creditscore_df(df: pandas.DataFrame):
    '''
    :param df: dataframe
    :return:
    '''

    credit_score_df = df[["creditscore", "geography", "gender", "exited"]].groupby(["gender", "geography"]).agg( \
        avg_credit_score=("creditscore", np.mean),
        sum_exited=("exited", np.sum)
    )
    credit_score_df.reset_index(inplace=True)
    credit_score_df.sort_values(by="avg_credit_score", inplace=True, ascending=False)
    return credit_score_df


def create_exited_age_correlation(df):
    exited_age = df.groupby(["geography", "gender", "exited"]).agg(
        total_exited=("exited", np.sum),
        mean_age=("age", lambda x: int(np.mean(x))),
        estimated_salary=("estimatedsalary", np.mean)

    )
    exited_age.reset_index(inplace=True)
    exited_age.sort_values(by="estimated_salary", ascending=False)
    return exited_age


def create_exited_salary_correlation(df):
    exited_salary = df[['geography', 'gender', 'exited', 'estimatedsalary']].groupby(['geography', 'gender']).agg(
        estimated_salary=('estimatedsalary', np.mean)
    )
    exited_salary.reset_index(inplace=True)
    min_salary = exited_salary['estimated_salary'].min()
    df["is_greater"] = (df["estimatedsalary"] > min_salary).astype("int")
    df['correlation'] = (df['exited'] == (~df["is_greater"].astype("int"))).astype("int")

    correlation_df = pd.DataFrame({
        "exited": df["exited"],
        "is_greater": df["is_greater"],
        "correlation": df['correlation']
    })
    return correlation_df


def create_tables():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modeling_creditscore (gender VARCHAR(50), geography VARCHAR(50),
        avg_credit_score FLOAT,sum_exited INTEGER )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS chrun_modeling_age_exited_correlation ( geography VARCHAR(50),
        gender VARCHAR(50),exited INTEGER ,total_exited INTEGER ,mean_age INTEGER, estimated_salary FLOAT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modeling_exited_salary ( exited INTEGER ,is_greater INTEGER , 
        correlation INTEGER)""")
    except Exception as e:
        logging.info(f"an error occurred when creating the tables {e}")


def write_credit_score_df_to_db(df: pandas.DataFrame):
    query = """INSERT INTO churn_modeling_creditscore (gender,geography,avg_credit_score,sum_exited) VALUES (%s,%s,%s,%s)"""
    row_count = 0
    for _, row in df.iterrows():
        values = (row["gender"], row["geography"], row["avg_credit_score"], row["sum_exited"])
        cur.execute(query,values)
        row_count += 1
        if row_count % 10 == 0:
            logging.info(f"{row_count} rows inserted into table churn_modeling creditscore")


def write_exited_age_to_db(df: pandas.DataFrame):
    query = """INSERT INTO chrun_modeling_age_exited_correlation (geography,gender,exited,total_exited,mean_age,estimated_salary) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count = 0
    for _, row in df.iterrows():
        values = (row['geography'], row['gender'], int(row['exited']), int(row['total_exited']), int(row['mean_age']),
                  float(row['estimated_salary']))
        cur.execute(query, values)
        row_count += 1
        if row_count % 10 == 0:
            logging.info(f"{row_count} rows inserted into table churn_modelling_exited_age_correlation")


def write_exited_salary_correlation_table(df):
    query = """INSERT INTO churn_modeling_exited_salary (exited,is_greater,correlation) VALUES (%s, %s, %s)"""
    row_count = 0
    for _, row in df.iterrows():
        values = (int(row['exited']), int(row['is_greater']), int(row['correlation']))
        cur.execute(query, values)
        row_count += 1
        if row_count % 1000 == 0:
            logging.info(f"{row_count} rows inserted into table churn_modelling_exited_age_correlation")


def main():
    df = extract_data_from_db()
    create_tables()
    creditscore_df = create_creditscore_df(df)
    df_exited_age_correlation = create_exited_age_correlation(df)
    df_exited_salary_correlation = create_exited_salary_correlation(df)
    write_credit_score_df_to_db(creditscore_df)
    write_exited_age_to_db(df_exited_age_correlation)
    write_exited_salary_correlation_table(df_exited_salary_correlation)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
