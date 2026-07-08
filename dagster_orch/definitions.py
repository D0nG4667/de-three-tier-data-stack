from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_orch.project import dbt_project
from dagster_orch.assets import raw_readings, cleaned_readings, dbt_warehouse, mongodb_sample

# Bundle all Software-Defined Assets and resources into the Dagster Code Location definition
defs = Definitions(
    assets=[raw_readings, cleaned_readings, dbt_warehouse, mongodb_sample],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
