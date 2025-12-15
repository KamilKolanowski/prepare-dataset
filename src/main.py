from helpers import data_manager

def process_fact_payroll():
    dm = data_manager.DataManager("./src/data/input/FactEmployeePayroll.csv", 32)
    fact_payroll = dm.generate_fact_employee_payroll(2025, 4)
    
    dm.save_df_to_csv(fact_payroll, "ES", "FACT001")

def process_dim_employee():
    dm = data_manager.DataManager("./src/data/input/DimEmployee.csv", 32)
    dim_employee = dm.generate_dim_employee()

    dm.save_df_to_csv(dim_employee, "ES", "DIM001")

if __name__ == "__main__":
    process_fact_payroll()
    process_dim_employee()
    