from pathlib import Path
from dagster_dbt import DbtProject

# Canonical reference mapping to the dbt project directory
dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "dbt_project").resolve(),
)

# Automatically compile dbt project and generate manifest.json at startup in local dev
dbt_project.prepare_if_dev()
