import pyodbc
import pandas as pd
from sql.netezza_sql import *


def get_netezza_df(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date):
    conn = pyodbc.connect(
        "Driver={NetezzaSQL};server=bx1-prd-ibmpd; PORT=5480;Database=ANALYSIS_DB;UID=chopdah;PWD=orange2021;")
    # "DRIVER={NetezzaSQL};SERVER=192.168.0.10; PORT=5480;DATABASE=TESTDB; UID=admin;PWD=password;")
    try:
        cus = conn.cursor()
        # Execute SQL statement and store result in cursor
        print("Started querying netezza.... please wait")
        run_sql = get_setup1(set_renewal_start_date, set_renewal_end_date)
        cus.execute(run_sql)
        run_sql = get_setup2()
        cus.execute(run_sql)
        run_sql = get_setup3()
        cus.execute(run_sql)
        run_sql = get_setup4(set_invite_start_date, set_invite_end_date)
        cus.execute(run_sql)
        run_sql = "select * from gipp_base;"
        try:
            data = pd.read_sql(run_sql, conn)
            print(data)
            # data.LASTTXN_QUOTE_REFERENCE = data.LASTTXN_QUOTE_REFERENCE.apply('="{}"'.format)
            # data.INVITE_QUOTE_REFERENCE = data.INVITE_QUOTE_REFERENCE.apply('="{}"'.format)
            data.to_csv("../results/RenewalBreakdown.csv")
        finally:
            print("done")

    finally:
        print("done")
