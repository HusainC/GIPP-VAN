from typing import List
from datetime import date
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


def read_sql(f) -> List[str]:
    """
    Reads a sql file and returns a list of the individual queries as strings so they can be executed sequentially
    :param f:
    :return List:
    """
    with open(f, "r") as f:
        return [query + ";" for query in f.read().split(";")[:-1]]


def get_sql_results(cs, path: str) -> None:
    """
    Reads the case breakdown sql file and executes the queries before storing the results as a csv
    :param cs: Snowflake cursor
    :param path: Path of the sql file being read
    :return: Writes csv with final snowflake query results
    """
    name = path.split("/")[-1][:-4]
    queries = read_sql(path)
    for query in queries:
        cs.execute(query)
    df = cs.fetch_pandas_all()
    """
    Currently adding in a date so we have a record while running things
    Probably need to add a if cs.fetch_pandas_all(): df =, else print("no results")
    Just need to work out logic first
    """
    df.to_csv(f"../results/{name}_{date.today().strftime('%Y-%m-%d')}")
