from pathlib import Path

from airflow.models import DagBag


DAGS_FOLDER = Path(__file__).parent.parent / "dags"


def test_dag_imports():

    dagbag = DagBag(
        dag_folder=str(DAGS_FOLDER),
        safe_mode=True,
    )

    assert len(dagbag.dags) > 0, "No DAGs found!"

    assert not dagbag.import_errors, (
        f"DAG import errors: {dagbag.import_errors}"
    )