import snowflake.connector
from builder import *


def main():
    set_renewal_start_date = "2022-04-19"
    set_renewal_end_date = "2022-04-19"
    set_invite_start_date = "2022-03-21"
    set_invite_end_date = "2022-03-21"

    con = snowflake.connector.connect(
        user='husainchopdawala@hastingsdirect.com',
        password='Hastings_2020',
        account='hstsf01.eu-west-1')
    cur = con.cursor()

    # scenario_one(cur, set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date)
    # scenario_two(cur, set_renewal_start_date, set_renewal_end_date, set_invite_start_date)
    # scenario_three(cur, set_renewal_start_date, set_renewal_end_date, set_invite_start_date, set_invite_end_date)
    # scenario_four_one(cur, set_renewal_start_date, set_renewal_end_date, set_invite_start_date)
    # runs next day
    builder(cur, set_renewal_start_date, set_renewal_end_date, con)

    return 0


if __name__ == '__main__':
    main()