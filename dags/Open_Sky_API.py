##
# @author: Bernard Polidario
# #

from datetime import datetime, date
from time import mktime
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import requests
from typing import List

BASE_URL = "https://opensky-network.org/api"


def to_seconds_since_epoch(input_date: str) -> int:
    return int(mktime(date.fromisoformat(input_date).timetuple()))

@dag(
    schedule=None,
    start_date=datetime(2022, 12, 1),
    catchup=False
)
def fetch_and_process_flights():
    params = {
        "airport": "LFPG",  # ICAO code for CDG
        "begin": to_seconds_since_epoch("2022-12-01"),
        "end": to_seconds_since_epoch("2022-12-02")
    }

    cdg_flights = f"{BASE_URL}/flights/departure"

    def reading() -> List:
        response = requests.get(cdg_flights, params=params)

        flights = response.json()
        # print("The type of flights is: " + str(type(flights)))
        # print(flights)

        return flights

    def writing(flights: List):
        with open("flights.json", "w+") as f:
            f.write(str(flights))

    read_first = PythonOperator(
        task_id='reading',
        python_callable=reading
    )

    write_second = PythonOperator(
        task_id='writing',
        python_callable=writing,
        op_kwargs={'flights': read_first.output}
    )

    read_first >> write_second

_ = fetch_and_process_flights()