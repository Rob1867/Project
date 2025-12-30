
import pandas as pd

vi_filepath = r'C:\Users\robert.everitt\OneDrive - National Grid\Data Engineering Course\Data\VI_E0309B_Test.xlsx'

df_vi = pd.read_excel(vi_filepath)

def clean_dataframe(df, search_text):

    # Find the index of the first occurrence of the search text
    idx = df[df.apply(lambda row: row.astype(str).str.contains(search_text, case=False).any(), axis=1)].index
    
    if idx.empty:
        raise ValueError(f"'{search_text}' not found in the DataFrame.")
    
    # Get the first matching index
    first_match_idx = idx[0]
    
    # Update the DataFrame by removing rows above the match
    df = df.iloc[first_match_idx:].reset_index(drop=True)
    
    # Set the first row as column names and drop it from the DataFrame
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    
    return df

df_vi_clean = clean_dataframe(df_vi,'Location Code')

df_vi_clean.to_csv('out.csv', index=False)  

import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="file", context_root_dir=".")

data_source = context.data_sources.add_or_update_pandas_filesystem(
    name="data_source", base_directory="./gx"
)

data_asset = data_source.add_csv_asset(name="data_asset")

batch_def = data_asset.add_batch_definition_path(
    name="batch_def", path="out.csv"
)

suite = context.suites.add_or_update(gx.ExpectationSuite(name="VI_data_suite"))

# TODO: Add more expectations
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="Script Activity ID"))

Installed_componets = ["VT", "PI", "DIS", "ESW", "ERT", "CSE", "CT", "TAP", "HVB", "GCB", "SGT", "SA", "BLDS", "VCT", "ABC", "NOI",
    "LVB", "FNCE", "GAN", "ROAD", "TRCH", "OCB", "BUS", "EAT", "BUC", "BUA", "BUB", "COM", "PNL", "AUX", "QB",
    "REA", "ENV", "GZ", "0", "TXM", "MSC", "SPI", "CT", "BB", "GEN", "HES", "CBR", "LIT", "FIL"
]

suite.add_expectation(gxe.ExpectColumnDistinctValuesToBeInSet(column="Installed Component",value_set=Installed_componets))
#suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="quantity",min_value=1.0,max_value=None))

batch = batch_def.get_batch()
print(batch.head())

print(batch.validate(suite))

context.build_data_docs()
context.open_data_docs()