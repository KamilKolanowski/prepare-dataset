from collections.abc import Iterable
from typing import List, Iterator
from datetime import datetime, timedelta
from itertools import cycle, islice
from calendar import monthrange
from decimal import Decimal
from faker import Faker

import polars as pl
import pycountry
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
    
    def generate_new_employee_ids(self, col_name: str) -> Iterable[int]:
        max_employee_id: int = (
            self.read_file()
                .select(pl.col(col_name).max())
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

    def generate_dates(self, start_date: datetime, end_date: datetime, how_many: int) -> Iterator[List[int]]:
        delta_days = (end_date - start_date).days

        for _ in range(how_many):
            random_days = random.randint(0, delta_days)
            birth_date = start_date + timedelta(days=random_days)
            yield [int(birth_date.strftime("%Y%m%d"))]
            

    def generate_fact_employee_payroll(self, year: int, month: int):
        employee_ids = list(self.generate_new_employee_ids("EmployeeId"))
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
    
    def generate_dim_employee(self) -> pl.DataFrame:
        employee_ids = list(self.generate_new_employee_ids("EmployeeSourceId"))

        first_names = [self.fake.first_name().upper() for _ in range(self.rows_amt)]
        last_names = [self.fake.last_name().upper() for _ in range(self.rows_amt)]
        middle_names = [self.fake.first_name().upper() if random.random() < 0.2 else "" for _ in range(self.rows_amt)]
        full_names = [
            f"{f} {m + ' ' if m else ''}{l}" for f, m, l in zip(first_names, middle_names, last_names)
        ]

        work_emails = [
            f"{f.lower()}.{l.lower()}@workmail.com".upper() for f, l in zip(first_names, last_names)
        ]

        national_ids = [random.choice([c.alpha_2 for c in pycountry.countries]).upper() for _ in range(self.rows_amt)]
        citizenship_codes = national_ids.copy()

        birth_dates = [
            bd[0] for bd in self.generate_dates(
                start_date=datetime(1940, 1, 1), 
                end_date=datetime(2008, 1, 1), 
                how_many=self.rows_amt
            )
        ]

        sex = [random.randint(0, 1) for _ in range(self.rows_amt)]
        is_working = [random.randint(0, 1) for _ in range(self.rows_amt)]
        is_on_absence = [0] * self.rows_amt
        is_suspended = [0] * self.rows_amt
        is_student = [random.randint(0, 1) for _ in range(self.rows_amt)]
        is_juvenile = [0] * self.rows_amt
        has_disability = [0] * self.rows_amt

        hire_dates = [
            hd[0] for hd in self.generate_dates(
                start_date=datetime(2010, 1, 1),
                end_date=datetime(2025, 12, 31),
                how_many=self.rows_amt
            )
        ]

        termination_dates = ["TERMINATION_DATE"] * self.rows_amt
        termination_reasons = ["TERMINATION_REASON_CODE"] * self.rows_amt

        positions = [random.choice(["Employee", "Manager", "Intern", "Contractor"]).upper() for _ in range(self.rows_amt)]
        levels = [random.randint(0, 5) for _ in range(self.rows_amt)]
        cost_center_ids = list(self.extract_list_of_random_values_from_file("CostCenterID"))

        localizations = ["R021"] * self.rows_amt
        employee_group_ids = ["E"] * self.rows_amt
        employee_group_names = ["EMPLOYEE"] * self.rows_amt
        departments_lvl1 = ["OPS"] * self.rows_amt
        departments_lvl2 = ["OPS Crew"] * self.rows_amt
        departments_lvl3 = [""] * self.rows_amt
        departments_lvl4 = [""] * self.rows_amt
        departments_lvl5 = [""] * self.rows_amt
        seniority_days = [random.randint(0, 25000) for _ in range(self.rows_amt)]

        supervisor_indices = [random.choice(range(len(employee_ids))) for _ in range(self.rows_amt)]
        supervisor_ids = [employee_ids[i] for i in supervisor_indices]
        supervisor_first_names = [first_names[i] for i in supervisor_indices]
        supervisor_middle_names = [middle_names[i] for i in supervisor_indices]
        supervisor_last_names = [last_names[i] for i in supervisor_indices]
        supervisor_full_names = [
            f"{f} {m + ' ' if m else ''}{l}" for f, m, l in zip(supervisor_first_names, supervisor_middle_names, supervisor_last_names)
        ]

        df = pl.DataFrame({
            "EmployeeSourceId": employee_ids,
            "FirstName": first_names,
            "MiddleName": middle_names,
            "LastName": last_names,
            "FullName": full_names,
            "WorkEmail": work_emails,
            "NationalId": national_ids,
            "CitizenshipCode": citizenship_codes,
            "BirthDate": birth_dates,
            "Sex": sex,
            "IsWorking": is_working,
            "IsOnAbsence": is_on_absence,
            "IsSuspended": is_suspended,
            "IsStudent": is_student,
            "IsJuvenile": is_juvenile,
            "HasDisability": has_disability,
            "HireDate": hire_dates,
            "TerminationDate": termination_dates,
            "TerminationReasonCode": termination_reasons,
            "Position": positions,
            "Level": levels,
            "CostCenterId": cost_center_ids,
            "Localization": localizations,
            "SupervisorId": supervisor_ids,
            "SupervisorFirstName": supervisor_first_names,
            "SupervisorMiddleName": supervisor_middle_names,
            "SupervisorLastName": supervisor_last_names,
            "SupervisorFullName": supervisor_full_names,
            "EmployeeGroupId": employee_group_ids,
            "EmployeeGroupName": employee_group_names,
            "DepartmentLvl1": departments_lvl1,
            "DepartmentLvl2": departments_lvl2,
            "DepartmentLvl3": departments_lvl3,
            "DepartmentLvl4": departments_lvl4,
            "DepartmentLvl5": departments_lvl5,
            "SeniorityDays": seniority_days
        })

        return df
    
    def save_df_to_csv(self, df: pl.DataFrame, country: str, table_name: str):
        df.write_csv(f"./src/data/output/PAYROLL_AMR_{country}001_{table_name}_D{datetime.now().strftime('%Y%m%d')}.csv")