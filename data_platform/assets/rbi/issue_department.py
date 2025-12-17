import os
from dagster import asset, Output
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError
from dagster_dbt import dbt_assets, DbtCliResource
from src.rbi.excel_reader import read_issue_department_excel

DBT_PROJECT_DIR = Path("dbt")

def get_max_period_date(conn, table_name: str):
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT MAX(period_date) FROM {table_name}"
        )
        return cursor.fetchone()[0]
    except ProgrammingError as e:
        # Table does not exist (first run)
        if "does not exist" in str(e):
            return None
        raise
    finally:
        cursor.close()

@asset(
    name="issue_department_raw",
    description="RBI Issue Department liabilities & assets (monthly) from Excel source",
    key_prefix=["rbi"], # Given this prefix to adjust with dbt source name for the proper lineage.
)
def rbi_issue_department_raw(context) -> Output:
    file_path = Path("source/RBI - Liabilities & Assets.xlsx")

    # Read full Excel snapshot
    df = read_issue_department_excel(file_path)
    context.log.info(f"Read {len(df)} rows from RBI Excel")

    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    table_name = "ISSUE_DEPARTMENT_RAW"

    try:
        # Find max already loaded period
        max_loaded_period = get_max_period_date(conn, table_name)

        context.log.info(
            f"Max loaded period in Snowflake: {max_loaded_period}"
        )

        # Incremental filter
        if max_loaded_period:

            result_df = df[df["PERIOD_DATE"] > max_loaded_period]
        else:
            result_df = df  # first run

        # Write only if new data exists
        rows_inserted = 0
        if not result_df.empty:
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=result_df,
                table_name=table_name,
                auto_create_table=True,
            )
            rows_inserted = nrows
            context.log.info(
                f"Inserted {nrows} new rows into {table_name}"
            )
        else:
            context.log.info("No new periods found — skipping insert")

    finally:
        conn.close()

    # Always return metadata
    return Output(
        None,
        metadata={
            "table_name": table_name,
            "rows_in_excel": len(df),
            "rows_inserted": rows_inserted,
            "latest_period_in_excel": str(df["PERIOD_DATE"].max()),
            "max_loaded_period_before_run": str(max_loaded_period),
            "load_type": "incremental"
        }
    )


@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    )
def rbi_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
