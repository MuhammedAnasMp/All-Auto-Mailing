import pandas as pd
from datetime import datetime
# Define your required columns
REQUIRED_VALUE_COLUMNS = ["ARTICLE_CODE", "SU", "UNIQ_NAME","UNIQ_CODE","FROM_DATE", "TO_DATE", "FLYER_RSP", "FLYER_TYPE", "CREATED_BY" , "APPLICABLE_LOCATIONS"]
REQUIRED_COLUMNS = [ "ARTICLE_CODE", "SU", "UNIQ_CODE", "UNIQ_NAME", "FROM_DATE", "TO_DATE", "FLYER_RSP", "REG_RSP", "UNIT_DN", "REMARKS", "FLYER_TYPE", "CREATED_BY", "APPLICABLE_LOCATIONS"]
DATE_COLUMNS = ["FROM_DATE","TO_DATE"]
date_format = "%d-%b-%y"
from openpyxl import load_workbook
path = r"C:\Users\HP\Downloads\testinh\FLYER_LIST  - 14 - 20 JAN - 26.xlsx"
# path = "TestFlareExcel.xlsx"
all_sheets = pd.read_excel(path, sheet_name=None) 
# all_sheets = {"TEST": all_sheets["TEST"]}
wb = load_workbook(path, data_only=True)
file_info = {"id": 222}
messages = []
INVALID_STRINGS = {"n/a", "na", "null", "none", "-"}
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter


for sheet_name, df in all_sheets.items():
    
    if df.empty:
        messages.append({
            "type": "warning",
            "message": f"Sheet '{sheet_name}' is empty",
            "id": file_info.get("id")
        })
        continue
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        messages.append({
            "type": "error",
            "message": f"Sheet '{sheet_name}': Missing required columns: {', '.join(missing_columns)}",
            "id": file_info.get("id")
        })

    # 1. Detect Row Types first
    def is_empty_row(row):
        return row.isna().all() or (row.astype(str).str.strip() == "").all()
    
    def is_last_item_only(row):
        """
        Returns True if all items in the row are null/empty 
        except for the very last column.
        """
        # Convert to a pandas Series if it isn't one already to use .isna()
        s = pd.Series(row)
        
        # prefix: all elements except the last one
        prefix = s.iloc[:-1]
        
        # last_item: the final element
        last_item = s.iloc[-1]
        
        # Condition 1: All elements in the prefix must be null
        prefix_is_empty = prefix.isna().all()
        
        # Condition 2: The last item must NOT be null
        last_is_not_empty = pd.notna(last_item)
        
        return prefix_is_empty and last_is_not_empty

    # 2. Create masks for the whole dataframe
    # This checks if a cell is NaN, empty string, or an INVALID_STRING
    invalid_mask = df.copy()
    for col in df.columns:
        invalid_mask[col] = (
            df[col].isna() | 
            (df[col].astype(str).str.strip() == "") | 
            (df[col].astype(str).str.lower().isin(INVALID_STRINGS))
        )

    previous_row_empty = False
    for i, row in df.iterrows():
        excel_row = i + 2 
        if is_empty_row(row):
            previous_row_empty = True
            print(f"row > {excel_row} empty row")
            continue

        if is_last_item_only(row):
            print(f"row > {excel_row} last item only")

            if previous_row_empty:
                messages.append({
                    "type": "error",
                    "message": (
                        f"Sheet '{sheet_name}': "
                        f"Empty row found in {excel_row-1} "
                    ),
                    "id": file_info.get("id")
                })

            previous_row_empty = False
            continue

        # reset if normal row
        previous_row_empty = False
            
        for col in REQUIRED_VALUE_COLUMNS:
            if col not in df.columns:
                break

            ws = wb[sheet_name]
            excel_row = i + 2
            resolved_value = row[col]

            

            if invalid_mask.at[i, col]:

                
                excel_col_idx = list(df.columns).index(col) + 1
                cell_addr = f"{get_column_letter(excel_col_idx)}{excel_row}"

                # ---- MERGE CHECK ----
                is_merged = False
                

                for merged_range in ws.merged_cells.ranges:
                    if cell_addr in merged_range:
                        is_merged = True
                        min_col, min_row, _, _ = merged_range.bounds
                        resolved_value = ws.cell(row=min_row, column=min_col).value
                        break

                

          
                if is_merged and resolved_value is not None:
                    
                    if col in DATE_COLUMNS:
                        try:
                            if pd.isna(resolved_value):
                                raise ValueError("Empty date")

                            # ---- HANDLE TIMESTAMP / DATETIME ----
                            if isinstance(resolved_value, (pd.Timestamp, datetime)):
                                date_obj = resolved_value.date()

                            else:
                                # string or other → parse then take date
                                date_obj = pd.to_datetime(resolved_value).date()

                            # ---- FINAL STRICT FORMAT CHECK ----
                            datetime.strptime(date_obj.strftime(date_format), date_format)

                            continue  # ✅ valid date

                        except Exception as e: 
                            
                            messages.append({
                                "type": "error",
                                "message": (
                                    f"Sheet '{sheet_name}': Invalid date in column {e}"
                                    f"'{col}', row {excel_row}: {resolved_value}"
                                ),
                                "id": file_info.get("id")
                            })
                            continue



                    continue  # merged + has value = valid
                else:
                    messages.append({
                        "type": "error",
                        "message": (
                            f"Sheet '{sheet_name}': Row {excel_row}, "
                            f"Col '{col}' is missing or invalid"
                        ),
                        "id": file_info.get("id")
                    })
print(messages)

