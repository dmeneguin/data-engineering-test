# [START values_etl_dag]
# [START import_module]
import json
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import datetime
from sqlalchemy import create_engine
import os
import urllib.request
import time;

# [END import_module]

# [START default_args]
default_args = {
    'owner': 'airflow',
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'values_etl_dag',
    default_args=default_args,
    description='values dag',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=[],
) as dag:
    # [END instantiate_dag]

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
        formatted_df = formatted_df[["year_month", "uf", "product", "unit", "volume", "created_at"]]
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
        formatted_df["year_month"] = formatted_df["year_month"].astype('string')

    def create_and_fill_created_at_column(formatted_df):
        formatted_df["created_at"] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        formatted_df["created_at"] = formatted_df["created_at"].astype('string')

    def remove_unit_from_product_name(formatted_df):
        formatted_df["COMBUSTÍVEL"] = formatted_df.apply(lambda row: row["COMBUSTÍVEL"].replace("(m3)", ""), axis=1)

    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs['ti']
        url = 'http://xls_pivot_cache_loader:5000/download'
        file_name, headers = urllib.request.urlretrieve(url)
        gas_df = pd.read_excel(file_name, sheet_name="DPCache_m3")
        diesel_df = pd.read_excel(file_name, sheet_name="DPCache_m3_2")
        ti.xcom_push('gas_data', gas_df.to_json())
        ti.xcom_push('diesel_data', diesel_df.to_json())

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        ti = kwargs['ti']
        extract_gas_data_string = ti.xcom_pull(task_ids='extract', key='gas_data')
        extract_diesel_data_string = ti.xcom_pull(task_ids='extract', key='diesel_data')
        extract_gas_data = pd.DataFrame(json.loads(extract_gas_data_string))
        extract_diesel_data = pd.DataFrame(json.loads(extract_diesel_data_string))
        transformed_gas_data = pd.DataFrame()
        transformed_gas_data = group_months_into_single_volume_column(extract_gas_data, transformed_gas_data)
        transformed_diesel_data = pd.DataFrame()
        transformed_diesel_data = group_months_into_single_volume_column(extract_diesel_data, transformed_diesel_data)        
        fill_na_of_volume_with_zeros(transformed_gas_data)
        create_and_fill_year_month_column(transformed_gas_data)
        create_and_fill_unit_column(transformed_gas_data)
        remove_unit_from_product_name(transformed_gas_data)
        create_and_fill_created_at_column(transformed_gas_data)
        fill_na_of_volume_with_zeros(transformed_diesel_data) 
        create_and_fill_year_month_column(transformed_diesel_data)
        create_and_fill_unit_column(transformed_diesel_data)
        remove_unit_from_product_name(transformed_diesel_data)
        create_and_fill_created_at_column(transformed_diesel_data)
        ti.xcom_push('transformed_gas_data', transformed_gas_data.to_json())
        ti.xcom_push('transformed_diesel_data', transformed_diesel_data.to_json())
        ti.xcom_push('original_gas_data', extract_gas_data.to_json())
        ti.xcom_push('original_diesel_data', extract_diesel_data.to_json())        

    # [END transform_function]

    # [START verification_function]
    def verify(**kwargs):
        ti = kwargs['ti']
        transformed_gas_data_string = ti.xcom_pull(task_ids='transform', key='transformed_gas_data')
        transformed_diesel_data_string = ti.xcom_pull(task_ids='transform', key='transformed_diesel_data')
        original_gas_data_string = ti.xcom_pull(task_ids='transform', key='original_gas_data')
        original_diesel_data_string = ti.xcom_pull(task_ids='transform', key='original_diesel_data')        
        transformed_gas_data = pd.DataFrame(json.loads(transformed_gas_data_string))
        transformed_diesel_data = pd.DataFrame(json.loads(transformed_diesel_data_string))
        original_gas_data = pd.DataFrame(json.loads(original_gas_data_string))
        original_diesel_data = pd.DataFrame(json.loads(original_diesel_data_string))
        verify_import_consistency(original_gas_data, transformed_gas_data)
        verify_import_consistency(original_diesel_data, transformed_diesel_data)

    # [END verification_function]

    # [START load_function]
    def load(**kwargs):
        ti = kwargs['ti']
        transformed_gas_data_string = ti.xcom_pull(task_ids='transform', key='transformed_gas_data')
        transformed_diesel_data_string = ti.xcom_pull(task_ids='transform', key='transformed_diesel_data')
        transformed_gas_data = pd.DataFrame(json.loads(transformed_gas_data_string))
        transformed_diesel_data = pd.DataFrame(json.loads(transformed_diesel_data_string))
        transformed_gas_data = extract_relevant_columns_and_rename(transformed_gas_data)
        transformed_diesel_data = extract_relevant_columns_and_rename(transformed_diesel_data)
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        transformed_gas_data.to_sql('gas_sales', engine, if_exists='replace', index=False)
        transformed_diesel_data.to_sql('diesel_sales', engine, if_exists='replace', index=False)
        with engine.connect() as con:
            con.execute('ALTER TABLE gas_sales ALTER COLUMN year_month TYPE date USING(year_month::date)')
            con.execute('ALTER TABLE diesel_sales ALTER COLUMN year_month TYPE date USING(year_month::date)')
            con.execute('ALTER TABLE diesel_sales ALTER COLUMN created_at TYPE timestamp USING(created_at::timestamp)')
            con.execute('ALTER TABLE gas_sales ALTER COLUMN created_at TYPE timestamp USING(created_at::timestamp)')
            con.execute('CREATE INDEX diesel_sales_idx ON diesel_sales (year_month, uf, product);')
            con.execute('CREATE INDEX gas_sales_idx ON diesel_sales (year_month, uf, product);')
            

    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    verify_task = PythonOperator(
        task_id='verify',
        python_callable=verify,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
    transform_task >> verify_task

# [END main_flow]

# [END values_etl_dag]
