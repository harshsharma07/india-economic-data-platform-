from dagster import Definitions
from dags.assets.rbi.issue_department import rbi_issue_department_raw
from dags.checks.rbi_issue_department_checks import period_date_not_null_check, no_duplicate_periods_check, negative_value_check, balance_sheet_balances_check

defs = Definitions(
    assets=[rbi_issue_department_raw],
    asset_checks=[period_date_not_null_check, no_duplicate_periods_check, negative_value_check, balance_sheet_balances_check]
)
