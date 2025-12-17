from dagster import Definitions
from dags.assets.rbi.issue_department import rbi_issue_department_raw, rbi_dbt_assets
from dags.jobs.rbi_pipeline_job import rbi_issue_department_pipeline_job
from dags.checks.rbi_issue_department_checks import period_date_not_null_check, no_duplicate_periods_check, negative_value_check, balance_sheet_balances_check
from dagster_dbt import DbtCliResource

defs = Definitions(
    assets=[rbi_issue_department_raw, rbi_dbt_assets],
    jobs=[rbi_issue_department_pipeline_job],
    asset_checks=[period_date_not_null_check, no_duplicate_periods_check, negative_value_check, balance_sheet_balances_check],
    resources={
        "dbt": DbtCliResource(project_dir="dbt_rbi"),
    },
)
