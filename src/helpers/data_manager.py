from collections.abc import Iterable
from typing import List, Iterator
from datetime import datetime, timedelta
from itertools import cycle, islice
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

    
    def generate_random_decimals(self, l_digs: int, r_digs: int, h_amt: bool = False) -> Iterator[Decimal]:
        for _ in range(self.rows_amt):
            if h_amt:
                base = self.fake.random_int(min=0, max=10**l_digs - 1)
                half = self.fake.random_element([0, 0.5])
                yield Decimal(base + half)
            else:
                yield Decimal(
                    self.fake.pydecimal(
                        left_digits=l_digs,
                        right_digits=r_digs,
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

    def generate_fact_employee_payroll(self, year: int, month: int):
        employee_ids = list(self.generate_new_employee_ids())
        cost_center_ids = list(self.extract_list_of_random_values_from_file("CostCenterID"))
        wage_component_codes = list(self.extract_list_of_random_values_from_file("WageComponentCode"))
        pay_group_codes = list(self.extract_list_of_random_values_from_file("PayGroupCode"))
        salaries = [float(x) for x in self.generate_random_decimals(4, 2)]
        hours = [float(x) for x in self.generate_random_decimals(2, 0, True)]
        payroll_dates = list(self.generate_payroll_dates(year, month, 6))

        payroll_dates_repeated = list(islice(cycle(payroll_dates), len(employee_ids)))

        payout_start = [x[0] for x in payroll_dates_repeated]
        payout_end = [x[1] for x in payroll_dates_repeated]
        payroll_date = [x[2] for x in payroll_dates_repeated]
        payroll_number = [x[3] for x in payroll_dates_repeated]
        currency_codes = ["EUR"] * len(employee_ids)

        fact_employee_payroll = pl.DataFrame({
            "EmployeeId": employee_ids,
            "CostCenterId": cost_center_ids,
            "WageComponentCode": wage_component_codes,
            "PayGroupCode": pay_group_codes,
            "PayoutStartDate": payout_start,
            "PayoutEndDate": payout_end,
            "PayrollDate": payroll_date,
            "PayrollNumber": payroll_number,
            "PayoutAmount": salaries,
            "PayoutAmountEuro": salaries,
            "CurrencyCode": currency_codes,
            "HoursAmount": hours,
        })

        return fact_employee_payroll
    
    def save_df_to_csv(self, df: pl.DataFrame, country: str, table_name: str):
        df.write_csv(f"./src/data/output/PAYROLL_AMR_{country}001_{table_name}_D{datetime.now().strftime('%Y%m%d')}.csv")