from helpers import data_manager



if __name__ == "__main__":
    dm = data_manager.DataManager("./src/data/FactEmployeePayroll.csv", 4)
    print(list(dm.generate_new_employee_ids()))
    print(list(dm.extract_list_of_random_values_from_file("WageComponentCode")))
    print(list(dm.generate_random_decimals()))
    print()
    print(list(dm.generate_payroll_dates(2025, 4, 6)))
    