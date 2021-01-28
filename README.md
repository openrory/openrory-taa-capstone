# Introduction

These are instructions for running data ingestion tasks through Apache Airflow as an orchestrator, to improve transparency and traceability.

# Contributors

- Airflow: Rory Sie (rory.sie@gmail.com)
- Event Hubs producer/consumer: Bas Jongewaard (bas.jongewaard@ortec.com)

# Requirements
- Apache Airflow https://airflow.apache.org/docs/apache-airflow/stable/start.html
- `pip install apache-airflow`
- If using a different folder than `~/airflow/dags` for your DAGs, follow these steps to add several DAG folders: https://medium.com/@xnuinside/how-to-load-use-several-dag-folders-airflow-dagbags-b93e4ef4663c (or edit and move [misc\add_dagbags.py]() to ~/airflow/dags and run `python3 ~/airflow/dags/add_dagbags.py`)

# Running Airflow DAGs

1. Start the Gremlin server:
  1. Open a cmd and type: `cd C:\Program Files\Azure Cosmos DB Emulator`
  1. `Microsoft.Azure.Cosmos.Emulator.exe /EnableGremlinEndpoint`
  1. `cd /d C:\sdk\apache-tinkerpop-gremlin-console-3.3.4-bin\apache-tinkerpop-gremlin-console-3.3.4`
  1. `bin\gremlin.bat`
  1. `:remote connect tinkerpop.server conf/remote-localcompute.yaml`
  1. `:remote console`
1. Ensure you have a DAG specified, for instance [dags\airflow-dag.py]()
1. Start the webserver in a terminal: `$ airflow webserver -p 8080`
1. Select the DAG and click Trigger ('play' button). Now watch you workflow run.
