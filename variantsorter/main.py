import pandas as pd
import numpy as np

try:
    source_df = pd.read_csv("3m-stuff-for-data-processing.csv", encoding="cp1252")
except FileNotFoundError:
    print(
        "Error: file not found. Please make sure the file is in the correct directory.")
    exit()


# Helper function to split option name and value
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


# Create new columns for option names and values
if 'Variant Option1 Name / Value' in source_df.columns:
    source_df['Variant Option1 Name'], source_df['Variant Option1 Value'] = split_option_info(
        source_df['Variant Option1 Name / Value'])

if 'Variant Option2 Name / Value' in source_df.columns:
    source_df['Variant Option2 Name'], source_df['Variant Option2 Value'] = split_option_info(
        source_df['Variant Option2 Name / Value'])

option_columns_to_process = []
if 'Variant Option1 Name / Value' in source_df.columns:
    option_columns_to_process.append("Variant Option1")
if 'Variant Option2 Name / Value' in source_df.columns:
    option_columns_to_process.append("Variant Option2")

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
    group_option_ids = {}

    option_counter = 1
    for option_prefix in option_columns_to_process:
        option_name_col = f"{option_prefix} Name"
        option_value_col = f"{option_prefix} Value"

        option_values = group_data[option_value_col].dropna().unique()

        # If there are no valid values for this option in this group, skip it entirely.
        if len(option_values) == 0:
            continue

        option_name = group_data[option_name_col].dropna().unique()[0]

        # Add Option Info
        option_id = f"{group_id}-{option_counter}"
        group_option_ids[option_prefix] = option_id
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

    if 'Sub Group' in group_data.columns:
        product_grouping = group_data.groupby(["InputSKU", "Sub Group"], dropna=False)
    else:
        product_grouping = group_data.groupby(["InputSKU"], dropna=False)

    for group_keys, sub_group_data in product_grouping:
        sku = group_keys if isinstance(group_keys, str) else group_keys[0]

        combination_ids_for_product = []
        for option_prefix in option_columns_to_process:
            option_value_col = f"{option_prefix} Value"

            if option_prefix not in group_value_maps:
                continue

            if option_value_col in sub_group_data.columns:
                # Use .iloc[0] because we are in a subgroup of one product
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

target_df.to_csv("wow-even-more-3m-stuff-huh-david.csv", index=False)

print("Script finished. The output is saved in the david folder.")