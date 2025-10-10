import pandas as pd
import json


def examine_excel_data(file_name):
    """Examine Excel file structure and content"""
    try:
        # Get all sheet names
        xl_file = pd.ExcelFile(file_name)
        sheet_names = xl_file.sheet_names

        print(f"Excel file: {file_name}")
        print(f"Number of sheets: {len(sheet_names)}")
        print(f"Sheet names: {sheet_names}")
        print("\n" + "=" * 50 + "\n")

        # Examine each sheet
        for sheet in sheet_names:
            print(f"SHEET: {sheet}")
            print("-" * 30)

            # Read first few rows to understand structure
            df = pd.read_excel(file_name, sheet_name=sheet, nrows=10)

            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 5 rows:")
            print(df.head())
            print("\nData types:")
            print(df.dtypes)
            print("\n" + "=" * 50 + "\n")

    except Exception as e:
        print(f"Error examining Excel file: {str(e)}")


if __name__ == "__main__":
    examine_excel_data("data-simplified.xlsx")
