from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys


def load_dbt_runner_module():
    module_path = Path("airflow/dags/lib/dbt_runner.py")
    spec = spec_from_file_location("dbt_runner", module_path)
    module = module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_ensure_dbt_profile_writes_expected_profile(tmp_path) -> None:
    module = load_dbt_runner_module()
    module.DBT_PROFILES_DIR = tmp_path / ".dbt"
    module.DBT_TARGET_PATH = tmp_path / "warehouse" / "analytics.duckdb"

    profile_path = module.ensure_dbt_profile()

    assert profile_path.exists()
    content = profile_path.read_text(encoding="utf-8")
    assert "taxi_lakehouse:" in content
    assert "type: duckdb" in content
