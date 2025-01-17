"""Reads from a local Postgres database connection and writes to BigQuery.

Environment prerequisites:
- Java
- Local Postgres (v12.22) running: `brew services start postgresql@12`

Check commands:
- Enter database: `psql -U postgres -d test_db`
- Check table: `\d`
- Check records: `SELECT * FROM dummy_table;`
- Postgres version check: `SELECT version();`

Run:
    python3 main.py
"""
import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc

from apache_beam.typehints.schemas import MillisInstant
from apache_beam.typehints.schemas import LogicalType

# https://stackoverflow.com/questions/77317904/apache-beam-python-sdk-reading-from-postgres-using-jdbc-io
LogicalType.register_logical_type(MillisInstant)

def run_pipeline():
    with beam.Pipeline(options=None) as pipeline:
        (
            pipeline
            # Ref: https://beam.apache.org/releases/pydoc/2.44.0/apache_beam.io.jdbc.html
            | 'ReadFromPostgres' >> ReadFromJdbc(
                table_name="dummy_table",
                driver_class_name="org.postgresql.Driver",
                jdbc_url="jdbc:postgresql://localhost:5432/test_db",
                username="postgres",
                password="postgres",
                query="SELECT * FROM dummy_table",
            )
            # Example output:
            # BeamSchema_345a7125_0693_4e68_b144_4c2fbf1528c5(id=1, name='Alice', created_at=Timestamp(1737075733.372000))
            # BeamSchema_345a7125_0693_4e68_b144_4c2fbf1528c5(id=2, name='Bob', created_at=Timestamp(1737075733.372000))
            # BeamSchema_345a7125_0693_4e68_b144_4c2fbf1528c5(id=3, name='Charlie', created_at=Timestamp(1737075733.372000))
            | 'PrintRecords' >> beam.Map(print)
        )


# Run the pipeline
if __name__ == '__main__':
    run_pipeline()
