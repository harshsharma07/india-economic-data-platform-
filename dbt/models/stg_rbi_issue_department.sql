{{ config(
    materialized="table",
    alias="stg_rbi_issue_department",
) }}


select
    period_date,
    notes_in_circulation,
    notes_held_in_banking_dept,
    total_liabilities,
    gold,
    foreign_securities,
    rupee_coin,
    goi_rupee_securities
from {{ source('rbi', 'issue_department_raw') }}