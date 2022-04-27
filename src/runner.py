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
    set_renewal_start_date = "2022-05-15"
    set_renewal_end_date = "2022-05-15"
    set_invite_start_date = "2022-03-21"
    set_invite_end_date = "2022-03-21"

    con = snowflake.connector.connect(
        user='husainchopdawala@hastingsdirect.com',
        password='Hastings_2020',
        account='hstsf01.eu-west-1')
    cur = con.cursor()

    part1(cur, set_renewal_start_date, set_renewal_end_date, con)
    #part2(set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date, con)

    return 0


if __name__ == '__main__':
    main()