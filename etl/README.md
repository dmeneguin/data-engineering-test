# ANP Fuel Sales ETL

### Integrations
This project has 4 main containers on docker-compose.yaml:
  - xls_pivot_cache_loader
  - libreoffice
  - airflow
  - pgadmin

The xls_pivot_cache_loader is a Python Flask app that downloads the [xls file](http://www.anp.gov.br/arquivos/dados-estatisticos/vendas-combustiveis/vendas-combustiveis-m3.xls), open it using Libreoffice UNO API (that is up in another container) and save it with all pivot cache loaded as worksheets, than the app serves it in the /download endpoint.

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/integration-diagram-libreoffice.jpg)

The Apache airflow is up in another container that has the etl_dag.py script. This script has a task for extract, transform, load and verify the data. For the extraction task, the endpoint /download served by xls_pivot_cache_loader app is consumed.

The pgadmin has a postgresql admin instance to make queries on postgresql database.

### ETL Overview
The ETL DAG has 4 tasks: Extraction, Transform, Verify, Load

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/diagram-airflow-tasks.png)

#### Extraction
Once LibreOffice loads pivot cache and saves the xls file, the worksheets look like this:

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/worksheets.png)

The DPCache_m3 sheet has oil derivative fuels data and DPCache_m3_2 has diesel data. Once these are loaded as pandas dataframes, they are passed as xcom parameters for the transform task.
#### Transform
The transform task uses pandas for the transposal of volumes columns (separated in months) into a single column called volume with another column called month, indicating the month that the row refers.

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/transform_task.jpg)

This task also uses the month and year columns to make the year_month column, all NA values of volumes are filled with zeros, a unit column is created and the unit is removed from the name of the product.
#### Verify
The verify task calculates the sum of volumes of all months filtered by product, state and year of the transformed dataframe and compares it with the original dataframe
#### Load
The load task removes the intermediary columns and renames the relevant ones. It stores the diesel data in the diesel_values table and oil derivative fuels into gas_values table. The types of year_month and created_at are corrected to date and timestamp respectively with sql alter table queries. Finally, a index is created for both tables, considering year_month, uf and product columns.

