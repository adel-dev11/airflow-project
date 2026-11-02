# 📦 Daily Orders Report — Airflow ETL Project

An end-to-end **ETL pipeline** built with **Apache Airflow** and **PostgreSQL**. It runs daily to extract order data, validate it, and load a daily summary into a reporting table.

The entire environment runs with **Docker Compose**, including two independent PostgreSQL databases. A new user can clone the repository and start the project without manually creating databases, tables, or connections.

---

## 🗂️ Project Structure

```text
airflow-project/
├── .github/
│   └── workflows/
│       └── ci.yml
├── dags/
│   ├── daily_orders_report.py
│   ├── ETL_airflow .py
│   └── my_first_dag.py
├── init/
│   └── init.sql                  # Initializes source database and tables
├── tests/
│   └── test_dag_imports.py
├── logs/
├── plugins/
├── config/
├── docker-compose.yaml
├── .env                          # Local environment variables (not committed)
├── .env.example                  # Environment variable template
└── .gitignore
```

---

## 💡 What Does This Project Do?

The pipeline runs daily at midnight and executes three tasks in sequence:

```text
extract  ──►  transform  ──►  load
```

| Task          | Description                                                                                        |
| ------------- | -------------------------------------------------------------------------------------------------- |
| **Extract**   | Connects to the source PostgreSQL database and counts orders placed on the logical execution date. |
| **Transform** | Validates the extracted count and rejects negative values.                                         |
| **Load**      | Inserts or updates the daily summary in `daily_orders_summary`.                                    |

The pipeline uses Airflow XCom to pass data between tasks.

### DAG Graph

![DAG Graph](daily_orders_report_byadel-graph.png)

---

## 🐘 Database Architecture

The project uses two independent PostgreSQL services.

### 1. Airflow Metadata Database

* Service: `postgres`
* Image: `postgres:16`
* Database: `airflow`
* Purpose: Stores Airflow metadata, task states, and workflow information.
* Volume: `postgres-db-volume`

### 2. Source Database

* Service: `source-postgres`
* Image: `postgres:16`
* Database: `orders_db`
* Username: `orders_user`
* Purpose: Stores the source orders and daily reporting table.
* Volume: `source-postgres-volume`

The source database is initialized automatically using:

```text
init/init.sql
```

This script creates the following tables:

**Source table:**

```sql
CREATE TABLE IF NOT EXISTS "Orders" (
    "OrderID" INTEGER PRIMARY KEY,
    "CustomerID" VARCHAR(20) NOT NULL,
    "OrderDate" DATE NOT NULL,
    "TotalAmount" NUMERIC(10, 2) NOT NULL
);
```

It also inserts 20 sample orders with dates distributed across recent days.

**Destination table:**

```sql
CREATE TABLE IF NOT EXISTS daily_orders_summary (
    report_date DATE PRIMARY KEY,
    total_orders INTEGER NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

The pipeline uses `ON CONFLICT` to update an existing daily summary instead of inserting duplicate rows.

---

## ⚙️ Tech Stack

| Technology           | Purpose                                         |
| -------------------- | ----------------------------------------------- |
| Apache Airflow 3.3.2 | Workflow orchestration and scheduling           |
| PostgreSQL 16        | Airflow metadata and source/reporting databases |
| Redis 7.2            | Celery message broker                           |
| CeleryExecutor       | Distributed task execution                      |
| Python               | ETL logic                                       |
| Docker Compose       | Container orchestration                         |
| GitHub Actions       | Continuous Integration                          |
| Pytest               | DAG import validation                           |

---

## 🚀 How to Run the Project

### 1. Prerequisites

* Docker Desktop installed and running.
* At least 4 GB RAM and 2 CPUs available for Docker.
* Git installed.

### 2. Clone the Repository

```bash
git clone <your-repo-url>
cd airflow-project
```

### 3. Configure Environment Variables

Create your local `.env` file from the example:

```bash
cp .env.example .env
```

On Windows PowerShell, you can use:

```powershell
Copy-Item .env.example .env
```

Generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Update the values in `.env`:

```env
AIRFLOW_UID=50000
FERNET_KEY=<your-generated-key>
AIRFLOW_CONN_POSTGRES_ORDERS_DB=postgresql://orders_user:orders_password@source-postgres:5432/orders_db
```

The connection URI configures the Airflow connection automatically.

> **Security:** Never commit your real `.env` file or Fernet key. Keep `.env` in `.gitignore` and commit only `.env.example`.

### 4. Start the Environment

```bash
docker compose up -d
```

Docker Compose starts:

* Airflow metadata PostgreSQL
* Source PostgreSQL
* Redis
* Airflow initialization
* Airflow API Server
* Airflow Scheduler
* Airflow DAG Processor
* Airflow Triggerer
* Airflow Worker

The source PostgreSQL database automatically executes `init/init.sql` when its data volume is initialized for the first time.

### 5. Check Container Health

```bash
docker compose ps
```

Wait until the services report healthy status where health checks are configured.

### 6. Access the Airflow UI

Open:

```text
http://localhost:8181
```

Default credentials:

| Field    | Value     |
| -------- | --------- |
| Username | `airflow` |
| Password | `airflow` |

### 7. Verify the Source Database

List the available tables:

```bash
docker compose exec source-postgres psql -U orders_user -d orders_db -c "\dt"
```

Check the number of source orders:

```bash
docker compose exec source-postgres psql -U orders_user -d orders_db -c 'SELECT COUNT(*) FROM "Orders";'
```

Expected result:

```text
 count
-------
    20
```

### 8. Verify the Airflow Connection

The connection is configured through:

```env
AIRFLOW_CONN_POSTGRES_ORDERS_DB
```

Check that Airflow recognizes it:

```bash
docker compose exec airflow-scheduler airflow connections get postgres_orders_db
```

The connection should point to:

| Setting         | Value                |
| --------------- | -------------------- |
| Connection ID   | `postgres_orders_db` |
| Connection Type | `postgres`           |
| Host            | `source-postgres`    |
| Database        | `orders_db`          |
| Username        | `orders_user`        |
| Port            | `5432`               |

### 9. Trigger the DAG

1. Open the Airflow UI.
2. Find `daily_orders_report_byadel`.
3. Enable the DAG if it is paused.
4. Trigger it manually.
5. Open the Grid view and monitor the task states.

Expected task sequence:

```text
extract  →  transform  →  load
```

All three tasks should finish with `Success`.

### 10. Verify the Final Report

Run:

```bash
docker compose exec source-postgres psql -U orders_user -d orders_db -c "SELECT * FROM daily_orders_summary;"
```

The result should contain a daily summary with a non-zero order count when source orders exist for that execution date.

---

## 🔄 DAG Configuration

| Property    | Value                          |
| ----------- | ------------------------------ |
| DAG ID      | `daily_orders_report_byadel`   |
| Schedule    | `@daily`                       |
| Start Date  | October 1, 2026                |
| Catchup     | `False`                        |
| Retries     | 2                              |
| Retry Delay | 1 minute                       |
| Tags        | `postgres`, `orders`, `report` |

---

## 🛑 Stop the Environment

Stop containers without deleting persistent data:

```bash
docker compose down
```

To delete containers and **all associated Docker volumes**, including both PostgreSQL databases:

```bash
docker compose down -v
```

> ⚠️ This permanently deletes the database data stored in those volumes. Use it only when you intentionally want a fresh environment.

---

## 🐛 Troubleshooting

| Problem                        | Possible Solution                                                                                   |
| ------------------------------ | --------------------------------------------------------------------------------------------------- |
| Airflow UI is not loading      | Wait for the API Server health check to pass.                                                       |
| Source PostgreSQL is unhealthy | Check `docker compose logs source-postgres`.                                                        |
| DAG fails during extraction    | Verify `postgres_orders_db` and the source database.                                                |
| Connection not found           | Check `AIRFLOW_CONN_POSTGRES_ORDERS_DB` in `.env` and recreate the Airflow containers.              |
| Orders table does not exist    | Check whether `init.sql` ran successfully and whether the source volume was initialized previously. |
| Not enough memory warning      | Increase Docker Desktop memory allocation.                                                          |

---

## 🧪 Tests & Continuous Integration

### Run Tests Locally

```bash
pytest -v
```

The DAG import test uses Airflow's `DagBag` to check that DAG files load without import errors.

### GitHub Actions CI

The GitHub Actions workflow runs on every push and:

1. Sets up Python 3.12.
2. Installs Apache Airflow 3.3.2 and required providers.
3. Runs the test suite using Pytest.
4. Validates DAG imports.

---

## 📝 Notes

* Airflow XCom is used to exchange small data between ETL tasks.
* The source database is independent of the Airflow metadata database.
* Docker volumes preserve PostgreSQL data across container restarts.
* The source database initialization script runs only when PostgreSQL initializes an empty data directory.
* The reporting timestamp is written using the `Africa/Cairo` timezone in the ETL task.
