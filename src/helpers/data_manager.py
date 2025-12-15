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
        df = pl.read_csv(
            self.path,
            has_header=True,
            separator=";",
            dtypes={
                "PayoutAmount": pl.Utf8,
                "PayoutAmountEuro": pl.Utf8,
                "HoursAmount": pl.Utf8
            }
        )

        for col in ["PayoutAmount", "PayoutAmountEuro", "HoursAmount"]:
            if col in df.columns:
                df = df.with_columns([
                    pl.Series(
                        name=col,
                        values=[-float(x[:-1]) if x.endswith("-") else float(x) for x in df[col]]
                    )
                ])

        return df


    
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

    def generate_dates(self, start_date: datetime, end_date: datetime) -> Iterator[List[int]]:
        delta_days = (end_date - start_date).days

        for _ in range(self.rows_amt):
            random_days = random.randint(0, delta_days)
            birth_date = start_date + timedelta(days=random_days)
            yield [int(birth_date.strftime("%Y%m%d"))]
            

    def generate_fact_employee_payroll(self, dim_employee_df: pl.DataFrame, year: int, month: int, lookup_path: str):
        employee_ids = dim_employee_df["EmployeeSourceId"].to_list()
        cost_center_ids = dim_employee_df["CostCenterId"].to_list()
        lookup_dm = DataManager(lookup_path, len(employee_ids))
        wage_component_codes = list(lookup_dm.extract_list_of_random_values_from_file("WageComponentCode"))
        pay_group_codes = list(lookup_dm.extract_list_of_random_values_from_file("PayGroupCode"))

        all_rows = []

        for emp_id, cost_center in zip(employee_ids, cost_center_ids):
            num_rows = random.choice([1, 1, 2, 3])
            used_dates = []

            for _ in range(num_rows):
                for pd in self.generate_payroll_dates(year, month, 6):
                    if pd not in used_dates:
                        used_dates.append(pd)
                        payroll_date = pd
                        break

                is_negative = random.random() < 0.1
                h = float(next(self.generate_random_decimals(2, 0, True)))
                s = float(next(self.generate_random_decimals(4, 2)))
                if is_negative:
                    h_str = f"{h}-"
                    s_str = f"{s}-"
                else:
                    h_str = str(h)
                    s_str = str(s)

                all_rows.append({
                    "EmployeeId": emp_id,
                    "CostCenterId": cost_center,
                    "WageComponentCode": random.choice(wage_component_codes),
                    "PayGroupCode": random.choice(pay_group_codes),
                    "PayoutStartDate": payroll_date[0],
                    "PayoutEndDate": payroll_date[1],
                    "PayrollDate": payroll_date[2],
                    "PayrollNumber": payroll_date[3],
                    "PayoutAmount": s_str,
                    "PayoutAmountEuro": s_str,
                    "CurrencyCode": "EUR",
                    "HoursAmount": h_str
                })

        return pl.DataFrame(all_rows)


    def generate_fact_employee_disability(self, dim_employee_df: pl.DataFrame, year: int, month: int) -> pl.DataFrame:
        all_records = []
        for emp_id in dim_employee_df["EmployeeSourceId"]:
            num_rows = random.choice([0, 1, 1, 2])
            used_ranges = []

            for _ in range(num_rows):
                start_date = next(self.generate_dates(datetime(year, month, 1), datetime(year, month, monthrange(year, month)[1])))[0]
                start_dt = datetime.strptime(str(start_date), "%Y%m%d")
                duration = random.randint(1, 60)
                end_dt = start_dt + timedelta(days=duration - 1)
                end_date = int(end_dt.strftime("%Y%m%d"))

                overlaps = any(start_dt <= e and end_dt >= s for s, e in used_ranges)
                if overlaps:
                    continue

                used_ranges.append((start_dt, end_dt))
                all_records.append({
                    "EmployeeId": emp_id,
                    "DisabilityId": random.randint(1, 5),
                    "StartDate": start_date,
                    "EndDate": end_date
                })

        return pl.DataFrame(all_records)


    def generate_fact_employee_absence(self, dim_employee_df: pl.DataFrame, year: int, month: int, lookup_path: str) -> pl.DataFrame:
        lookup_dm = DataManager(lookup_path, len(dim_employee_df))
        absence_codes = list(lookup_dm.extract_list_of_random_values_from_file("AbsenceCode"))

        all_records = []

        for emp_id in dim_employee_df["EmployeeSourceId"]:
            num_rows = random.choice([0, 1, 1, 2])
            used_ranges = []

            for _ in range(num_rows):
                absence_code = random.choice(absence_codes)
                start_date = next(self.generate_dates(datetime(year, month, 1), datetime(year, month, monthrange(year, month)[1])))[0]
                start_dt = datetime.strptime(str(start_date), "%Y%m%d")
                duration_days = random.randint(1, 30)
                end_dt = start_dt + timedelta(days=duration_days - 1)
                end_date = int(end_dt.strftime("%Y%m%d"))

                overlaps = any(start_dt <= e and end_dt >= s for s, e in used_ranges)
                if overlaps:
                    continue
                used_ranges.append((start_dt, end_dt))

                working_days = min(duration_days, (duration_days // 7) * 5 + duration_days % 7)
                working_hours = working_days * 8

                all_records.append({
                    "EmployeeId": emp_id,
                    "AbsenceCode": absence_code,
                    "StartDate": start_date,
                    "EndDate": end_date,
                    "Days": float(duration_days),
                    "WorkingDays": float(working_days),
                    "WorkingHours": float(working_hours)
                })

        return pl.DataFrame(all_records)

    def generate_dim_employee(self) -> pl.DataFrame:
        employee_ids = list(self.generate_new_employee_ids("EmployeeSourceId"))

        first_names = [self.fake.first_name().upper() for _ in range(self.rows_amt)]
        last_names = [self.fake.last_name().upper() for _ in range(self.rows_amt)]
        middle_names = [
            self.fake.first_name().upper() if random.random() < 0.2 else ""
            for _ in range(self.rows_amt)
        ]

        full_names = [
            f"{f} {m + ' ' if m else ''}{l}"
            for f, m, l in zip(first_names, middle_names, last_names)
        ]

        work_emails = [
            f"{f.lower()}.{l.lower()}@workmail.com".upper()
            for f, l in zip(first_names, last_names)
        ]

        national_ids = [
            random.choice([c.alpha_2 for c in pycountry.countries]).upper()
            for _ in range(self.rows_amt)
        ]

        birth_dates = self._random_dates(
            datetime(1940, 1, 1),
            datetime(2008, 1, 1)
        )

        hire_dates = self._random_dates(
            datetime(2010, 1, 1),
            datetime(2023, 12, 31)
        )

        termination_dates, termination_reasons = self._generate_termination()

        positions, levels = self._generate_positions_and_levels()

        (
            supervisor_ids,
            sup_fn,
            sup_mn,
            sup_ln,
            sup_full
        ) = self._generate_supervision(
            employee_ids,
            (first_names, middle_names, last_names),
            positions
        )

        dept_lvl1, dept_lvl2 = self._generate_departments(positions)

        return pl.DataFrame({
            "EmployeeSourceId": employee_ids,
            "FirstName": first_names,
            "MiddleName": middle_names,
            "LastName": last_names,
            "FullName": full_names,
            "WorkEmail": work_emails,
            "NationalId": national_ids,
            "CitizenshipCode": national_ids,
            "BirthDate": birth_dates,
            "HireDate": hire_dates,
            "TerminationDate": termination_dates,
            "TerminationReasonCode": termination_reasons,
            "Position": positions,
            "Level": levels,
            "SupervisorId": supervisor_ids,
            "SupervisorFirstName": sup_fn,
            "SupervisorMiddleName": sup_mn,
            "SupervisorLastName": sup_ln,
            "SupervisorFullName": sup_full,
            "CostCenterId": list(self.extract_list_of_random_values_from_file("CostCenterId")),
            "Localization": list(self.extract_list_of_random_values_from_file("Localization")),
            "EmployeeGroupId": ["E"] * self.rows_amt,
            "EmployeeGroupName": ["Empleados"] * self.rows_amt,
            "DepartmentLvl1": dept_lvl1,
            "DepartmentLvl2": dept_lvl2,
            "DepartmentLvl3": [""] * self.rows_amt,
            "DepartmentLvl4": [""] * self.rows_amt,
            "DepartmentLvl5": [""] * self.rows_amt,
            "SeniorityDays": [random.randint(0, 25000) for _ in range(self.rows_amt)],
        })
    
    def generate_dim_employee_contract(self, dim_employee_df: pl.DataFrame) -> pl.DataFrame:
        employee_ids = dim_employee_df["EmployeeSourceId"].to_list()
        cost_center_ids = dim_employee_df["CostCenterId"].to_list()
        
        num_rows = len(employee_ids)
        
        employee_contract_ids = list(range(1, num_rows + 1))
        currency_ids = ["EUR"] * num_rows
        contract_types = ["Fijo"] * num_rows

        contract_start_dates = [
            next(self.generate_dates(datetime(2010, 1, 1), datetime(2025, 12, 31)))[0]
            for _ in range(num_rows)
        ]

        contract_end_dates = []
        for start in contract_start_dates:
            if random.random() < 0.5:
                contract_end_dates.append(99991231)
            else:
                start_dt = datetime.strptime(str(start), "%Y%m%d")
                end_dt = start_dt + timedelta(days=random.randint(30, 365*5))
                contract_end_dates.append(int(end_dt.strftime("%Y%m%d")))
        
        pay_group_codes = [next(self.extract_list_of_random_values_from_file("PayGroupCode")) for _ in range(num_rows)]
        salaries = [float(x) for x in self.generate_random_decimals(4, 2)]
        full_time_equiv = [100] * num_rows
        is_annex = [None] * num_rows
        calendar_date_valid_for = [next(self.generate_dates(datetime(2025, 1, 1), datetime(2025, 12, 31)))[0] for _ in range(num_rows)]
        
        return pl.DataFrame({
            "EmployeeContractId": employee_contract_ids,
            "EmployeeId": employee_ids,
            "CurrencyId": currency_ids,
            "ContractTypeMainCode": contract_types,
            "EmployeeContractStartDate": contract_start_dates,
            "EmployeeContractEndDate": contract_end_dates,
            "PayGroupCode": pay_group_codes,
            "Salary": salaries,
            "FullTimeEquivalent": full_time_equiv,
            "CostCenterId": cost_center_ids,
            "IsAnnex": is_annex,
            "CalendarDateValidFor": calendar_date_valid_for
        })

    def save_df_to_csv(self, df: pl.DataFrame, country: str, table_name: str):
        df.write_csv(f"./src/data/output/PAYROLL_AMR_{country}001_{table_name}_D{datetime.now().strftime('%Y%m%d')}.csv")


    def _random_dates(self, start: datetime, end: datetime) -> list:
        return [
            next(self.generate_dates(start_date=start, end_date=end))[0]
            for _ in range(self.rows_amt)
        ]

    def _generate_termination(self):
        is_terminated = [random.random() < 0.25 for _ in range(self.rows_amt)]
        reason_pool = list(
            self.extract_list_of_random_values_from_file("TerminationReasonCode")
        )

        dates, reasons = [], []

        for terminated in is_terminated:
            if terminated:
                dates.append(
                    next(
                        self.generate_dates(
                            start_date=datetime(2024, 1, 1),
                            end_date=datetime(2025, 12, 31)
                        )
                    )[0]
                )
                reasons.append(random.choice(reason_pool))
            else:
                dates.append(None)
                reasons.append(None)

        return dates, reasons

    def _generate_positions_and_levels(self):
        positions = [
            random.choice(["Intern", "Contractor", "Employee", "Manager"])
            for _ in range(self.rows_amt)
        ]

        position_to_level = {
            "Intern": 1,
            "Contractor": 2,
            "Employee": 3,
            "Manager": 4
        }

        return positions, [position_to_level[p] for p in positions]

    def _generate_departments(self, positions: list):
        lvl1, lvl2 = [], []

        for pos in positions:
            if random.random() < 0.25:
                lvl1.append("RST")
                lvl2.append("RST")
            else:
                lvl1.append("OPS")
                lvl2.append("OPS Manager" if pos == "Manager" else "OPS Crew")

        return lvl1, lvl2

    def _generate_supervision(self, employee_ids, names, positions):
        indices = [random.choice(range(self.rows_amt)) for _ in range(self.rows_amt)]

        for idx in set(indices):
            positions[idx] = "Manager"

        ids = [employee_ids[i] for i in indices]
        fn, mn, ln = names

        full = [
            f"{f} {m + ' ' if m else ''}{l}"
            for f, m, l in zip(
                [fn[i] for i in indices],
                [mn[i] for i in indices],
                [ln[i] for i in indices]
            )
        ]

        return (
            ids,
            [fn[i] for i in indices],
            [mn[i] for i in indices],
            [ln[i] for i in indices],
            full
        )
