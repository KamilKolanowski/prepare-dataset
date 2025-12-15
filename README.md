# Payroll Dataset Preparation

This project generates payroll datasets, including dimension tables (`DimEmployee`, `DimEmployeeContract`) and fact tables (`FactEmployeePayroll`, `FactEmployeeAbsence`, `FactEmployeeDisability`). 
The data respects referential integrity between dimensions and facts.

## Requirements

- Python 3.10+
- Polars
- Faker
- PyCountry

## Installation

1. Clone the repository:

```bash
git clone https://github.com/KamilKolanowski/prepare-dataset.git
cd prepare-dataset

2. Install dependencies

```bash
pip install -r requirements.txt

3. Run the main pipeline

```bash
python main.py
