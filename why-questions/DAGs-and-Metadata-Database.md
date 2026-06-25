# Example DAGs and Metadata Database

## Why Do Multiple DAGs Appear Automatically?

After starting Airflow for the first time, you may notice many DAGs already present in the Airflow UI even though you have not created any DAG files.

This happens because the Docker Compose configuration enables Airflow's built-in example DAGs:

```yaml
AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
```

These example DAGs are provided by Apache Airflow to help users learn:

* DAG scheduling
* Task dependencies
* Operators
* Sensors
* Dynamic task mapping
* Task groups
* Data pipelines

Examples include DAGs such as:

```text
example_bash_operator
example_branch_operator
example_python_operator
example_task_group
tutorial
```

These DAGs are automatically loaded into the Airflow metadata database during startup.

---

## Disable Example DAGs

If you do not want the example DAGs to appear, update the Docker Compose configuration:

```yaml
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
```

After making the change, restart Airflow.

For a completely clean environment, recreate the metadata database:

```bash
docker compose down --volumes --remove-orphans
docker compose up airflow-init
docker compose up -d
```

After reinitialization, only your custom DAGs located in the `dags/` folder will be displayed.

---

## Airflow Metadata Database

Airflow stores its operational data in PostgreSQL.

The database is configured using:

```yaml
POSTGRES_USER: airflow
POSTGRES_PASSWORD: airflow
POSTGRES_DB: airflow
```

Connection details:

```text
Host: postgres
Port: 5432
Database: airflow
Username: airflow
Password: airflow
```

Inside Docker, Airflow connects using:

```text
postgresql+psycopg2://airflow:airflow@postgres/airflow
```

---

## What Is Stored in the Metadata Database?

The Airflow metadata database stores:

* Users and Roles
* DAG Definitions
* DAG Run History
* Task Instance History
* Variables
* Connections
* Logs Metadata
* Scheduler State
* Trigger Information

Example:

```text
Airflow User
    ↓
PostgreSQL Metadata Database
    ↓
Persisted Across Container Restarts
```

---

## Does User Data Persist After Restart?

Yes.

The PostgreSQL container stores data in a Docker volume:

```yaml
volumes:
  postgres-db-volume:
```

Because of this:

### Restarting Containers

```bash
docker compose down
docker compose up -d
```

Result:

```text
✓ Users remain
✓ DAG history remains
✓ Variables remain
✓ Connections remain
```

### Recreating Containers

```bash
docker compose down
docker compose up -d
```

Result:

```text
✓ Data remains
```

Containers are recreated, but the database volume is preserved.

### Removing Volumes

```bash
docker compose down --volumes
```

Result:

```text
✗ Users deleted
✗ DAG history deleted
✗ Variables deleted
✗ Connections deleted
✗ Metadata database deleted
```

This command removes the PostgreSQL volume and resets Airflow to a clean state.

---

## Accessing PostgreSQL Directly

Connect from inside the PostgreSQL container:

```bash
docker compose exec postgres psql -U airflow -d airflow
```

Useful PostgreSQL commands:

```sql
\l        -- List databases
\dt       -- List tables
\du       -- List users
\q        -- Quit
```

Example:

```sql
SELECT * FROM ab_user;
```

This table contains Airflow users created through the UI or CLI.

---

## Inspect Existing Airflow Users

List Airflow users:

```bash
docker compose exec airflow-apiserver airflow users list
```

Example output:

```text
id | username | email                    | first_name | last_name | roles
1  | airflow  | airflowadmin@example.com | Airflow    | Admin     | Admin
```

Passwords are stored as hashes and cannot be viewed.

To change a password:

```bash
docker compose exec airflow-apiserver airflow users reset-password \
  --username airflow \
  --password MyNewPassword123
```
