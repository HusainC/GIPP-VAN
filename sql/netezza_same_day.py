import pyodbc
import pandas as pd
from sql.netezza_sql import netezza_queries


def get_netezza_df(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date):
    """Docstring goes brrr"""
    print("Connecting to Netezza...")
    conn = pyodbc.connect(
        "Driver={NetezzaSQL};server=bx1-prd-ibmpd; PORT=5480;Database=ANALYSIS_DB;UID=chopdah;PWD=orange2021;")
    cs = conn.cursor()
    # Execute SQL statement and store result in cursor
    print("Started querying netezza... please wait")
    queries = netezza_queries(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date)
    for query in queries:
        cs.execute(query)
    df = pd.read_sql("select * from van_gipp_base;", conn)
    print(df.head())
    df.to_csv("../results/RenewalBreakdown.csv")


"""def netezza_test(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date):
    print("Connecting to Netezza...")
    conn = pyodbc.connect(
        "Driver={NetezzaSQL};server=bx1-prd-ibmpd; PORT=5480;Database=ANALYSIS_DB;UID=chopdah;PWD=orange2021;")
    cs = conn.cursor()
    print("Testing pol sold")
    cs.execute(pol_sold(set_renewal_start_date, set_renewal_end_date))
    df = pd.read_sql("select * from pol_sold;", conn)
    print(df.head)
    print("Testing pol live")
    cs.execute(pol_live())
    df2 = pd.read_sql("select * from pol_live;", conn)
    print(df2.head)
    print("Testing invites")
    cs.execute(invites())
    df3 = pd.read_sql("select * from invites;", conn)
    print(df3.head)
    print("Testing gipp base")
    cs.execute(gipp_base(set_invite_start_date, set_invite_end_date))
    df4 = pd.read_sql("select * from gipp_base;", conn)
    print(df4.head)
"""