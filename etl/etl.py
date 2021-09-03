import pandas as pd
import datetime
from sqlalchemy import create_engine

def verify_import_consistency(original_df, imported_df):
    for i, row in original_df.iterrows():
        imported_df_filtered = imported_df[(imported_df["COMBUSTÍVEL"] == row["COMBUSTÍVEL"])  & (imported_df["ANO"] == row["ANO"]) & (imported_df["ESTADO"] == row["ESTADO"])]
        volume_sum = imported_df_filtered['volume'].sum()
        original_volume_sum = row[4:16].sum()
        if(round(volume_sum) != round(original_volume_sum)):
            print('The values of this set are mismatching: {} {} {} {} {}'.format(row["COMBUSTÍVEL"], row["ANO"], row["ESTADO"], volume_sum, original_volume_sum))

def reorganize_month_column_and_extract_subset(original_df, i, month):
    subset_df = original_df[["COMBUSTÍVEL", "ANO", "ESTADO", month]]
    subset_df = subset_df.rename(columns={ month: "volume"})
    subset_df["month"] = i + 1
    return subset_df

def extract_relevant_columns_and_rename(formatted_df):
    formatted_df = formatted_df.rename(columns={"COMBUSTÍVEL": "product", "ESTADO": "uf"})
    formatted_df = formatted_df[["year_month", "uf", "product", "unit", "volume"]]
    return formatted_df

def fill_na_of_volume_with_zeros(formatted_df):
    formatted_df["volume"] = formatted_df["volume"].fillna(0)

def group_months_into_single_volume_column(original_df, formatted_df):
    months_list = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    for i, month in enumerate(months_list):
        subset_df = reorganize_month_column_and_extract_subset(original_df, i, month)
        formatted_df = pd.concat([formatted_df, subset_df], axis=0).reset_index(drop=True)
    return formatted_df

def create_and_fill_unit_column(formatted_df):
    formatted_df["unit"] = "m3"

def create_and_fill_year_month_column(formatted_df):
    formatted_df["year_month"] = formatted_df.apply(lambda row: datetime.datetime(row["ANO"], row["month"], 1), axis=1)

def remove_unit_from_product_name(formatted_df):
    formatted_df["COMBUSTÍVEL"] = formatted_df.apply(lambda row: row["COMBUSTÍVEL"].replace("(m3)", ""), axis=1)


original_df = pd.read_excel("./data/vendas-combustiveis-m3.xls", sheet_name="DPCache_m3_2")
formatted_df = pd.DataFrame()
formatted_df = group_months_into_single_volume_column(original_df, formatted_df)
fill_na_of_volume_with_zeros(formatted_df)
verify_import_consistency(original_df, formatted_df)
create_and_fill_year_month_column(formatted_df)
create_and_fill_unit_column(formatted_df)
remove_unit_from_product_name(formatted_df)
formatted_df = extract_relevant_columns_and_rename(formatted_df)

engine = create_engine('postgresql://unicorn_user:magical_password@localhost:5432/rainbow_database')
formatted_df.to_sql('diesel_sales', engine, if_exists='replace', index=False)
