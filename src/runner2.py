import snowflake.connector
from sql.netezza_same_day import get_netezza_df
from report_generation.response import *
from report_generation.report_gen import final_report_gen
from builder import *
from datetime import datetime, date, timedelta
from util import send_completion_email
from datetime import datetime


def part1(cur, set_renewal_start_date, set_renewal_end_date, con):
    builder(cur, set_renewal_start_date, set_renewal_end_date, con)


def part2(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date, con):
    get_netezza_df(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date)
    get_full_report_s4()  # Probably should rename this function
    final_report_gen(con)




def main_updated() -> int:
    """
    # Prompt user for credentials
    print("Please enter snowflake credentials")
    user = input("email: ")
    pswd = getpass.getpass("password: ")
    """

    # Connecting to snowflake
    con = snowflake.connector.connect(
        user='jameswatson@hastingsdirect.com',
        password='Database!123',
        account='hstsf01.eu-west-1')
    cur = con.cursor()

    """
    Configure dates, eventually this might run on today's invites with renewals starting +29 days, but for now
    it is lagging by a day due to availability of data
    """
    renewal_start = renewal_end = (date.today() + timedelta(days=28)).strftime("%Y-%m-%d")
    invite_start = invite_end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    # renewal_start = renewal_end = '2022-08-28'
    # invite_start = invite_end = '2022-07-30'
    # Run the first part
    # print("Part 1 - ENB requests")
    # part1(cur, renewal_start, renewal_end, con)
    """
    print("Finished part one, waiting for db to update")

    Wait until db has been updated for part two
    print("Waiting for database to update, checking in 30 minute intervals")
    t1 = datetime.now()
    waiter = True
    while waiter:
        time.sleep(1800)  # 30 mins
        t2 = cur.execute("SELECT MAX(InsertTimeStamp) FROM PRD_RAW_DB.QUOTES_PUBLIC.VW_EARNIX_REQ_BASE;").fetchone()[0]
        if datetime.strptime(t2, "%Y-%m-%d %H:%M:%S.%f") > t1:
            waiter = False
    """
    # un the second part
    # print("Part 2 - report generation")
    part2(renewal_start, renewal_end, invite_start, invite_end, con)
    #
    # Send completion email
    # send_completion_email()

    return 0


if __name__ == '__main__':
    start = datetime.now()
    main_updated()
    end = datetime.now()
    print("Start time: ", start.strftime("%H:%M:%S"))
    print("End time: ", end.strftime("%H:%M:%S"))
    print("Time taken: ", (end - start))
