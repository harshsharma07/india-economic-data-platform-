from dagster import AssetSelection, define_asset_job

rbi_issue_department_pipeline_job = define_asset_job(
    name="rbi_issue_department_pipeline",
    selection=AssetSelection.assets(["rbi","issue_department_raw"]).downstream(),
)