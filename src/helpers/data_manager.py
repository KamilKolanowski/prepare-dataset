from collections.abc import Iterable
from typing import List, Iterator
from datetime import datetime, timedelta
from calendar import monthrange
from decimal import Decimal
from faker import Faker

import polars as pl
import random


class DataManager:
    def __init__(self, path: str, rows_amt: int):
        self.path = path
        self.rows_amt = rows_amt
        self.fake = Faker()


    def read_file(self):
        return pl.read_csv(
            self.path,
            has_header=True,
            separator=";"
        )
    
    def extract_column_names(self):
        headers = self.read_file().columns
        return headers
    
    def generate_new_employee_ids(self) -> Iterable[int]:
        max_employee_id: int = (
            self.read_file()
                .select(pl.col("EmployeeId").max())
                .item()
        )

        for i in range(1, self.rows_amt + 1):
            yield max_employee_id + i

    def extract_list_of_random_values_from_file(self, col_name: str):
        values_list: List[str] = (
            self.read_file()
                .select(pl.col(col_name).unique())
                .to_series()
                .to_list()
        )

        for i in range(self.rows_amt):
            yield random.choice(values_list)

    def generate_random_decimals(self) -> Iterator[Decimal]:
        for _ in range(self.rows_amt):
            yield Decimal(
                self.fake.pydecimal(
                    left_digits=4,
                    right_digits=2,
                    positive=True
                )
            )
    
    def generate_payroll_dates(self, start_year: int, start_month: int, months_count: int) -> Iterator[List[int]]:
        year = start_year
        month = start_month

        for _ in range(months_count):
            start_date = datetime(year, month, 1)
            last_day = monthrange(year, month)[1]
            end_date = datetime(year, month, last_day)
            payroll_date = end_date - timedelta(days=1)
            payroll_number = int(start_date.strftime("%Y%m"))

            yield [
                int(start_date.strftime("%Y%m%d")),
                int(end_date.strftime("%Y%m%d")),
                int(payroll_date.strftime("%Y%m%d")),
                payroll_number
            ]

            if month == 12:
                month = 1
                year += 1
            else:
                month += 1