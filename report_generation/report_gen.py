from sql.report_sql import *


def add_to_sf(dataframe, conn):
    if not dataframe.empty:
        sql = "USE ROLE FG_RETAILPRICING"
        conn.cursor().execute(sql)
        sql = "use warehouse wrk_retailpricing_medium;"
        conn.cursor().execute(sql)
        sql = "use database wrk_retailpricing;"
        conn.cursor().execute(sql)
        sql = "use schema growth;"
        conn.cursor().execute(sql)
        table_name = 'GIPP_VAN_SUBS'
        schema = 'GROWTH'
        database = 'WRK_RETAILPRICING'

        # # Create the SQL statement to create or replace the table
        dataframe['RN_SUBMISSION'] = dataframe['RN_SUBMISSION'].astype(str)
        dataframe['RN_SUBMISSION'] = dataframe['RN_SUBMISSION'].apply(lambda x: x.zfill(10))
        dataframe['NB_SUBMISSION'] = dataframe['NB_SUBMISSION'].astype(str)
        dataframe['NB_SUBMISSION'] = dataframe['NB_SUBMISSION'].apply(lambda x: x.zfill(11))
        dataframe['NB_SUBMISSION'] = dataframe['NB_SUBMISSION'].str[1:]

        create_tbl_statement = "CREATE OR REPLACE TRANSIENT TABLE " + database + "." + schema + "." + table_name + " (\n"

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
        # f = conn.cursor().execute("select * from  WRK_RETAILPRICING.CAR.CVH") # what is this?
        success, nchunks, nrows, _ = write_pandas(conn, dataframe, database=database, schema=schema,
                                                  table_name=table_name)
        print(success)


def final_report_gen(con):
    cur = con.cursor()
    dataframe = pd.read_csv("../results/nets.csv")
    add_to_sf(dataframe, con)
    get_sql_results(cur, "../sql/case_breakdown.sql")
    get_sql_results(cur, "../sql/data_issues.sql")
