from helpers import data_manager

class ETLPipeline:
    def __init__(self, country: str):
        self.country = country
        self.dim_tables = {}
        self.dm_instances = {}

    def generate_dim_employee(self, src_path: str, rows_amt: int, file_name: str):
        dm = data_manager.DataManager(src_path, rows_amt)
        dim_employee = dm.generate_dim_employee()
        self.dim_tables["DimEmployee"] = dim_employee
        self.dm_instances["DimEmployee"] = dm
        dm.save_df_to_csv(dim_employee, self.country, file_name)

    def generate_fact_table(
        self,
        fact_name: str,
        src_path: str,
        file_name: str,
        year: int,
        month: int,
        employee_dim_key: str = "DimEmployee"
    ):
        if employee_dim_key not in self.dim_tables:
            raise ValueError(f"{employee_dim_key} not generated yet")

        dim_employee_df = self.dim_tables[employee_dim_key]
        dm = self.dm_instances[employee_dim_key]

        if fact_name == "FactEmployeePayroll":
            fact_df = dm.generate_fact_employee_payroll(dim_employee_df, year, month, lookup_path=src_path)
        elif fact_name == "FactEmployeeAbsence":
            fact_df = dm.generate_fact_employee_absence(dim_employee_df, year, month, lookup_path=src_path)
        elif fact_name == "FactEmployeeDisability":
            fact_df = dm.generate_fact_employee_disability(dim_employee_df, year, month)
        else:
            raise ValueError(f"Unknown fact table: {fact_name}")

        dm.save_df_to_csv(fact_df, self.country, file_name)

    def generate_dim_employee_contract(
        self,
        src_path: str,
        rows_amt: int,
        file_name: str,
        base_dim_key: str = "DimEmployee"
    ):
        if base_dim_key not in self.dim_tables:
            raise ValueError(f"{base_dim_key} not generated yet")

        base_df = self.dim_tables[base_dim_key]
        dm = data_manager.DataManager(src_path, rows_amt)
        dim_contract = dm.generate_dim_employee_contract(base_df)
        self.dim_tables["DimEmployeeContract"] = dim_contract
        dm.save_df_to_csv(dim_contract, self.country, file_name)


if __name__ == "__main__":
    country = "ES"
    rows_amt = 100

    pipeline = ETLPipeline(country)

    pipeline.generate_dim_employee(
        src_path="./src/data/input/DimEmployee.csv",
        rows_amt=rows_amt,
        file_name="DIM001"
    )

    pipeline.generate_fact_table(
        fact_name="FactEmployeePayroll",
        src_path="./src/data/input/FactEmployeePayroll.csv",
        file_name="FACT001",
        year=2025,
        month=4
    )

    pipeline.generate_fact_table(
        fact_name="FactEmployeeAbsence",
        src_path="./src/data/input/FactEmployeeAbsence.csv",
        file_name="FACT006",
        year=2025,
        month=4
    )

    pipeline.generate_fact_table(
        fact_name="FactEmployeeDisability",
        src_path="./src/data/input/FactEmployeeDisability.csv",
        file_name="FACT002",
        year=2025,
        month=4
    )

    pipeline.generate_dim_employee_contract(
        src_path="./src/data/input/DimEmployeeContract.csv",
        rows_amt=rows_amt,
        file_name="DIM009"
    )
