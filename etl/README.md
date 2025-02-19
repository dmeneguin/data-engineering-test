# ANP Fuel Sales ETL

### Instructions for installation and execution
On the /etl file run:

```sh
docker-compose up
```

When all containers are up, access  
```sh
http://localhost:8080
```
And then, the Apache Airflow login screen should be available:

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/airflow-login.png)

The username and password are "airflow".

Once logged in, the DAG list should be visible:

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/dag.png)

Hitting the play button, the dag is executed.

------------

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

------------

### ETL Overview
The ETL DAG has 4 tasks: Extraction, Transform, Verify, Load

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/diagram-airflow-tasks.png)

#### Extraction
Once LibreOffice loads pivot cache and saves the xls file, the worksheets look like this:

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/worksheets.png)

The DPCache_m3 sheet has oil derivative fuels data and DPCache_m3_2 has diesel data. Once these are loaded as pandas dataframes, they are passed as xcom parameters for the transform task.
#### Transform
The values of pivot cache imported from libreoffice are misaligned with the columns they should be at. In the image below there is an example, the values in red should all be under the TOTAL column.

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/desalignment.png)

On the transform task there is a function called correct_values_misplacement_on_pivot_cache that solves this issue, putting every value on the column it should be.

After the correction of misplacement, this task uses pandas for the transposal of volumes columns (separated in months) into a single column called volume with another column called month, indicating the month that the row refers.

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/transform_task.jpg)

This task also uses the month and year columns to make the year_month column, all NA values of volumes are filled with zeros, a unit column is created and the unit is removed from the name of the product.
#### Verify
The verify task calculates the sum of volumes of all months filtered by product, state and year of the transformed dataframe and compares it with the  TOTAL column of the original dataframe. If there is an incompatible value, it prints on the logs:

![](https://github.com/dmeneguin/data-engineering-test/blob/master/images/logs_verify.png)

#### Load
The load task removes the intermediary columns and renames the relevant ones. It stores the diesel data in the diesel_values table and oil derivative fuels into gas_values table. The types of year_month and created_at are corrected to date and timestamp respectively with sql alter table queries. Finally, a index is created for both tables, considering year_month, uf and product columns.

