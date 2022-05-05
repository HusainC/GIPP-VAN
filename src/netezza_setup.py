from snowflake.connector.pandas_tools import write_pandas

from sql.sql_van import *
import pyodbc
import pandas as pd
import snowflake.connector


def get_netezza_df_same_day(set_renewal_start_date, set_renewal_end_date):
    conn = pyodbc.connect(
        "Driver={NetezzaSQL};server=bx1-prd-ibmpd; PORT=5480;Database=ANALYSIS_DB;UID=chopdah;PWD=orange2021;")
    # "DRIVER={NetezzaSQL};SERVER=192.168.0.10; PORT=5480;DATABASE=TESTDB; UID=admin;PWD=password;")
    try:
        # Run the new Ed sql and store file and upload to snowflake
        cus = conn.cursor()
        # Execute SQL statement and store result in cursor
        print("Started querying netezza.... please wait")
        run_sql = "drop table analysis_db.op.gipp_van_base if exists;"
        cus.execute(run_sql)
        run_sql = "drop table analysis_db.op.gipp_address_van if exists;"
        cus.execute(run_sql)
        run_sql = "drop table analysis_db.op.gipp_invites_van if exists;"
        cus.execute(run_sql)
        run_sql = get_setup1_netezza(set_renewal_start_date, set_renewal_end_date)
        cus.execute(run_sql)
        run_sql = get_setup2_netezza()
        cus.execute(run_sql)
        run_sql = get_setup3_netezza()
        cus.execute(run_sql)
        run_sql = "select * from analysis_db.op.gipp_van_base;"
        try:
            data = pd.read_sql(run_sql, conn)
            print(data)
            data.INVITE_REFERENCE = data.INVITE_REFERENCE.astype("str")
            data.to_csv("../results/upload.csv", index=False)
        finally:
            print("done")

    finally:
        print("done")


def add_to_sf_sd(dataframe, conn):
    if not dataframe.empty:
        sql = "USE ROLE FG_RETAILPRICING"
        conn.cursor().execute(sql)
        sql = "use warehouse WRK_RETAILPRICING_MEDIUM;"
        conn.cursor().execute(sql)
        sql = "use database WRK_RETAILPRICING;"
        conn.cursor().execute(sql)
        sql = "use schema CAR;"
        conn.cursor().execute(sql)
        table_name = 'GIPP_MON_SUBS'
        schema = 'CAR'
        database = 'WRK_RETAILPRICING'

        # # Create the SQL statement to create or replace the table
        # dataframe['RN_SUBMISSION'] = dataframe['RN_SUBMISSION'].astype(str)
        dataframe['INVITE_REFERENCE'] = dataframe['INVITE_REFERENCE'].apply(lambda x: str(x).zfill(10))
        # dataframe['NB_SUBMISSION'] = dataframe['NB_SUBMISSION'].astype(str)
        # dataframe['NB_SUBMISSION'] = dataframe['NB_SUBMISSION'].apply(lambda x: x.zfill(11))
        # dataframe['NB_SUBMISSION'] = dataframe['NB_SUBMISSION'].str[1:]

        create_tbl_statement = "CREATE OR REPLACE TABLE " + database + "." + schema + "." + table_name + " (\n"

        # Loop through each column finding the datatype and adding it to the statement
        #
        for column in dataframe.columns:
            if (
                    dataframe[column].dtype.name == "int"
                    or dataframe[column].dtype.name == "int64"
            ):
                create_tbl_statement = create_tbl_statement + column + " int"
            elif dataframe[column].dtype.name == "object":
                create_tbl_statement = create_tbl_statement + column + " varchar(255)"
            elif dataframe[column].dtype.name == "datetime64[ns, UTC]":
                create_tbl_statement = create_tbl_statement + column + " 	date"
            elif dataframe[column].dtype.name == "float64":
                create_tbl_statement = create_tbl_statement + column + " float8"
            elif dataframe[column].dtype.name == "bool":
                create_tbl_statement = create_tbl_statement + column + " boolean"
            else:
                create_tbl_statement = create_tbl_statement + column + " varchar(16777216)"

            # If column is not last column, add comma, else end sql-query
            if dataframe[column].name != dataframe.columns[-1]:
                create_tbl_statement = create_tbl_statement + ",\n"
            else:
                create_tbl_statement = create_tbl_statement + ")"
        #
        # # Execute the SQL statement to create the table
        #
        conn.cursor().execute(create_tbl_statement)
        # f = conn.cursor().execute("select * from  demo_db.public.GIPP_VAN_SUBS")
        success, nchunks, nrows, _ = write_pandas(conn, dataframe, database=database, schema=schema,
                                                  table_name=table_name)
        print(success)


if __name__ == '__main__':
    set_renewal_start_date = "2022-04-04"
    set_renewal_end_date = "2022-04-05"
    get_netezza_df_same_day(set_renewal_start_date, set_renewal_end_date)
    con = snowflake.connector.connect(
        user='husainchopdawala@hastingsdirect.com',
        password='Hastings_2020',
        account='hstsf01.eu-west-1')
    cur = con.cursor()
    df = pd.read_csv('../results/upload.csv')
    add_to_sf_sd(df, con)
