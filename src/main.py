from helpers import data_manager

if __name__ == "__main__":
    dm = data_manager.DataManager("./src/data/input/FactEmployeePayroll.csv", 32)
   
    fact_payroll = dm.generate_fact_employee_payroll(2025, 4)
    dm.save_df_to_csv(fact_payroll, "ES", "FACT001")

    