import pandas as pd
from pathlib import Path


def read_issue_department_excel(file_path: str | Path) -> pd.DataFrame:
    df = pd.read_excel(
        file_path,
        sheet_name="Report 1",
        skiprows=7
    )

    # Select relevant rows & columns
    df = df.iloc[:-1, 1:9]

    df.columns = [
        "PERIOD_DATE",
        "NOTES_IN_CIRCULATION",
        "NOTES_HELD_IN_BANKING_DEPT",
        "TOTAL_LIABILITIES",
        "GOLD",
        "FOREIGN_SECURITIES",
        "RUPEE_COIN",
        "GOI_RUPEE_SECURITIES",
    ]

    df = df.dropna(axis=0, how="all")

    df["PERIOD_DATE"] = pd.to_datetime(df["PERIOD_DATE"]).dt.date

    return df
