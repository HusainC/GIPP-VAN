import snowflake.connector
from sql.netezza_same_day import *
from report_generation.response import *
from report_generation.report_gen import final_report_gen
from builder import *


def part1(cur, set_renewal_start_date, set_renewal_end_date, con):
    builder(cur, set_renewal_start_date, set_renewal_end_date, con)


def part2(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date, con):
    get_netezza_df(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date)
    get_full_report_s4()
    final_report_gen(con)


def main():
    set_renewal_start_date = "2022-08-08"
    set_renewal_end_date = "2022-08-08"
    set_invite_start_date = "2022-07-10"
    set_invite_end_date = "2022-07-10"

    con = snowflake.connector.connect(
        user='husainchopdawala@hastingsdirect.com',
        password='Training15',
        account='hstsf01.eu-west-1')
    cur = con.cursor()

    part1(cur, set_renewal_start_date, set_renewal_end_date, con)
    # print("Waiting for database to update, checking in 30 minute intervals")
    # t1 = datetime.now()
    # wait = True
    # while wait:
    #     time.sleep(1800)  # 30 mins
    #     t2 = cur.execute("SELECT MAX(InsertTimeStamp) FROM PRD_RAW_DB.QUOTES_PUBLIC.VW_EARNIX_VAN_REQ_BASE;").fetchone()[0]
    #     if datetime.strptime(t2, "%Y-%m-%d %H:%M:%S.%f") > t1:
    #         wait = False
    #
    # # Run the second part
    # print("Part 2 - report generation")
    #part2(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date, con)

    return 0


if __name__ == '__main__':
    main()