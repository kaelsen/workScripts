import pandas as pd
import numpy as np

try:
    source_df = pd.read_csv("3MKitVariantsForAkio.csv", encoding="cp1252")
except FileNotFoundError:
    print("Error: file not found. Please make sure the file is in the correct directory.")
    exit()


# Split option name and value
def split_option_info(series):
    split_data = series.astype(str).str.split(' ', n=1, expand=True)
    if split_data.shape[1] < 2:
        split_data = pd.concat([split_data, pd.DataFrame({1: np.nan}, index=split_data.index)], axis=1)

    option_names = split_data[0]
    option_values = split_data[1]

    is_na = series.isna()
    option_names[is_na] = None
    option_values[is_na] = None

    return option_names, option_values


# --- DYNAMIC COLUMN DETECTION ---
# This will find 'Variant Option1 Name / Value' through 'Variant Option4 Name / Value'
option_columns_to_process = []
for i in range(1, 5):  # Range 1 to 4
    col_name = f'Variant Option{i} Name / Value'
    if col_name in source_df.columns:
        # Create the Name and Value columns
        source_df[f'Variant Option{i} Name'], source_df[f'Variant Option{i} Value'] = split_option_info(
            source_df[col_name])
        option_columns_to_process.append(f"Variant Option{i}")

# Initialize target dataframe
columns = ["Group ID", "Group Name", "Product ID", "Combination ID", "Option ID",
           "Option Name", "Style on Page", "Style on Card", "Value ID", "Value Name",
           "Swatch Style", "Swatch Color 1", "Swatch Color 2", "Swatch Image", "SKU", "Internal ID"]
target_df = pd.DataFrame(columns=columns)

default_style = {"Style on Page": "Button", "Style on Card": "Button",
                 "Swatch Style": "1 Color", "Swatch Color 1": "#000",
                 "Swatch Color 2": "#141414"}

for group_id, group_data in source_df.groupby("Variant Parent / Group ID"):

    group_name = group_data["Input Product Name"].iloc[0].split("(")[0].strip()
    target_df = pd.concat([target_df, pd.DataFrame([{
        "Group ID": group_id,
        "Group Name": group_name,
        **{k: "" for k in columns[2:]}
    }])], ignore_index=True)

    group_value_maps = {}

    # Process each detected option (1 through 4)
    option_counter = 1
    for option_prefix in option_columns_to_process:
        option_name_col = f"{option_prefix} Name"
        option_value_col = f"{option_prefix} Value"

        option_values = group_data[option_value_col].dropna().unique()

        if len(option_values) == 0:
            continue

        option_name_vals = group_data[option_name_col].dropna().unique()
        option_name = option_name_vals[0] if len(option_name_vals) > 0 else "Unknown"

        # Add Option Info
        option_id = f"{group_id}-{option_counter}"
        target_df = pd.concat([target_df, pd.DataFrame([{
            "Group ID": group_id,
            "Option ID": option_id,
            "Option Name": option_name,
            **default_style
        }])], ignore_index=True)

        # Add Option Values and build value_map
        value_counter = 1
        current_value_map = {}
        for value in option_values:
            value_id = f"{option_id}-{value_counter}"
            current_value_map[value] = value_id
            target_df = pd.concat([target_df, pd.DataFrame([{
                "Group ID": group_id,
                "Option ID": option_id,
                "Value ID": value_id,
                "Value Name": value,
                **default_style
            }])], ignore_index=True)
            value_counter += 1

        group_value_maps[option_prefix] = current_value_map
        option_counter += 1

    # Product/SKU grouping logic
    group_cols = ["InputSKU"]
    if 'Sub Group' in group_data.columns:
        group_cols.append("Sub Group")

    product_grouping = group_data.groupby(group_cols, dropna=False)

    for group_keys, sub_group_data in product_grouping:
        sku = group_keys if isinstance(group_keys, str) else group_keys[0]

        combination_ids_for_product = []
        # loop through detected options to build the Combination ID string (e.g., ID1/ID2/ID3/ID4)
        for option_prefix in option_columns_to_process:
            option_value_col = f"{option_prefix} Value"

            if option_prefix in group_value_maps:
                option_value = sub_group_data[option_value_col].iloc[0]

                if pd.notna(option_value):
                    value_id = group_value_maps[option_prefix].get(option_value, "")
                    if value_id:
                        combination_ids_for_product.append(value_id)

        combined_combination_id = "/".join(combination_ids_for_product)

        if not sub_group_data.empty:
            target_df = pd.concat([target_df, pd.DataFrame([{
                "Group ID": group_id,
                "Product ID": sku,
                "Combination ID": combined_combination_id,
                "SKU": sub_group_data["SKU"].iloc[0],
                "Internal ID": sub_group_data["Internal ID"].iloc[0],
                **default_style
            }])], ignore_index=True)

target_df.to_csv("3M-Store-Variants-fom-David-More-Abuse.csv", index=False)
print("Script finished. The output is saved.")