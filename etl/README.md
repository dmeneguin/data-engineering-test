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


