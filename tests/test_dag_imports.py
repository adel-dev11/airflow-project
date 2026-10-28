import os
from airflow.models import DagBag


DAGS_FOLDER = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "dags")
)


def test_dag_imports():
    dagbag = DagBag(
        dag_folder=DAGS_FOLDER,
    )

    assert dagbag.import_errors == {}, (
        f"DAG import errors: {dagbag.import_errors}"
    )