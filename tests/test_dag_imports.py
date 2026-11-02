import os
from airflow.models import DagBag

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


def test_dag_imports():
    dagbag = DagBag(
        dag_folder=DAGS_FOLDER,
        include_examples=False,
    )

    assert dagbag.import_errors == {}, (
        f"DAG import errors: {dagbag.import_errors}"
    )