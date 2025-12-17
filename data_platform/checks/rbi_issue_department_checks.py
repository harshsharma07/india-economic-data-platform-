from dagster import AssetCheckResult, asset_check
from data_platform.assets.rbi.issue_department import rbi_issue_department_raw
import snowflake.connector
import os


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


@asset_check(asset=rbi_issue_department_raw, name="period_date_not_null")
def period_date_not_null_check(context) -> AssetCheckResult:
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM ISSUE_DEPARTMENT_RAW
            WHERE period_date IS NULL
        """)
        null_count = cursor.fetchone()[0]

        return AssetCheckResult(
            passed=null_count == 0,
            metadata={"null_period_date_count": null_count}
        )
    finally:
        cursor.close()
        conn.close()

@asset_check(asset=rbi_issue_department_raw, name="no_duplicate_periods")
def no_duplicate_periods_check(context) -> AssetCheckResult:
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM (
                SELECT period_date
                FROM ISSUE_DEPARTMENT_RAW
                GROUP BY period_date
                HAVING COUNT(*) > 1
            )
        """)
        duplicate_count = cursor.fetchone()[0]

        return AssetCheckResult(
            passed=duplicate_count == 0,
            metadata={"duplicate_period_count": duplicate_count}
        )
    finally:
        cursor.close()
        conn.close()

@asset_check(asset=rbi_issue_department_raw, name="balance_sheet_balances")
def balance_sheet_balances_check(context) -> AssetCheckResult:
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM ISSUE_DEPARTMENT_RAW
            WHERE ABS(
                total_liabilities -
                (gold + foreign_securities + rupee_coin + goi_rupee_securities)
            ) > 5
        """)
        mismatch_count = cursor.fetchone()[0]

        return AssetCheckResult(
            passed=mismatch_count == 0,
            metadata={"balance_mismatch_count": mismatch_count}
        )
    finally:
        cursor.close()
        conn.close()


@asset_check(asset=rbi_issue_department_raw, name="negative_value")
def negative_value_check(context) -> AssetCheckResult:
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM ISSUE_DEPARTMENT_RAW
            WHERE notes_in_circulation < 0
                OR gold < 0
                OR foreign_securities < 0
        """)
        negative_value_count = cursor.fetchone()[0]

        return AssetCheckResult(
            passed=negative_value_count == 0,
            metadata={"negative_value_count": negative_value_count}
        )
    finally:
        cursor.close()
        conn.close()