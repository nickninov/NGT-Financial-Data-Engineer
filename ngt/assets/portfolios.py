from dagster import asset, Output, AssetExecutionContext, AssetIn, MarkdownMetadataValue, MaterializeResult
from ..configs import RawFilesConfig
from ..resources import Mongo
from .. import constants
import datetime
import pandas as pd
import os

@asset(
    compute_kind="Pandas",
    description="Load the portfolio data into a DataFrame.",
    group_name="Portfolios_Upload"
)
def portfolios_file_data(context: AssetExecutionContext, config: RawFilesConfig) -> Output:
    
    data = pd.read_csv(config.file_path)
    context.log.info(f"Opened file {os.path.basename(config.file_path)}")
    
    metadata = {
        "rows": len(data),
        "preview": MarkdownMetadataValue(data.head(20).to_markdown(index=False)),
        "file_name": os.path.basename(config.file_path),
        "fund_codes": "; ".join(data["nt_pool_fund_code"].unique())
    }

    return Output(data, metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Prepare the raw data to be uploaded.",
    group_name="Portfolios_Upload",
    ins={
        "data": AssetIn("portfolios_file_data")
    }
)
def portfolios_raw_processed_data(context: AssetExecutionContext, data: pd.DataFrame) -> Output:

    def get_identifier(row: pd.Series) -> str:
        """
        Get the identifier for the current row
        """
        if not pd.isna(row["nt_figi_code"]):
            return row["nt_figi_code"]
        
        bbg_id = row["nt_bloomberg_code"]
        if pd.isna(bbg_id):
            return row["nt_security_name"].replace(" ", "_")
        
        
        if pd.isna(row["nt_bloomberg_code_of_underlying"]):
            return bbg_id
        
        return bbg_id + "/" + row["nt_bloomberg_code_of_underlying"]

    data["nx_date"] = pd.to_datetime(data["nx_date"])
    data["date"] = data["nx_date"] - pd.offsets.BDay()
    context.log.info("Fixed date columns")

    initial_rows = len(data)    
    data = data.drop_duplicates().reset_index(drop=True)
    context.log.info(f"Dropped duplicates (from {initial_rows} to {len(data)})")

    id = data["date"].dt.strftime("%Y-%m-%d") + "/" + data["nt_pool_fund_code"] + "/" + data["nt_issuer_country_code"] + "/" + data["nt_gti_code"] + "/" + data.apply(get_identifier, axis=1) + "/" + data["nt_quantity"].astype(str).str.replace("-", "NEG")
    data.insert(0, "id", id)
    context.log.info("Created unique ID")

    data["upload_timestamp"] = datetime.datetime.now()
    metadata = {
        "preview": MarkdownMetadataValue(data.head(20).to_markdown(index=False))
    }

    return Output(data, metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Create new columns.",
    group_name="Portfolios_Upload",
    ins={
        "data": AssetIn("uploaded_raw_portfolios")
    }
)
def new_portfolio_columns(context: AssetExecutionContext, data: pd.DataFrame, country_codes: pd.DataFrame) -> Output:
    
    if len(data) == 0:
        context.log.info(f"No data found")
        return Output(data)
    
    country_codes = country_codes.rename(columns={
        "country_name": "issuer_country",
        "country_code": "issuer_country_code"
    })

    # (1) Remove nt prefix
    data.columns = [column[3:] if column.startswith("nt_") else column for column in data.columns.to_list()]
    context.log.info(f"Removed \"nt_\" prefix:\n{data.columns}")

    # (2) Remove nx_date column
    data = data.drop("nx_date", axis=1)
    context.log.info("Removed \"nx_date\"")

    # (2) Remove 0 quantities
    missing_quantities = data["quantity"] == 0
    metadata = {
        "empty": int(missing_quantities.sum())
    }
    data = data.loc[~missing_quantities].reset_index(drop=True)
    context.log.info("Removed rows with quantity 0")

    # (3) Long / Short
    data["long_short"] = "short"
    data.loc[data["quantity"] > 0, "long_short"] = "long"
    context.log.info(f"Added long / short")

    metadata.update({
        "long": int((data["long_short"] == "long").sum()),
        "short": int((data["long_short"] == "short").sum())
    })

    # (4) Add country name
    data = data.merge(country_codes, "left", "issuer_country_code")
    metadata.update({
        "preview": MarkdownMetadataValue(
                data.groupby("issuer_country")\
                    .count()["id"].reset_index()\
                    .rename(columns={"id": "rows"})\
                    .sort_values("issuer_country")\
                    .to_markdown(index=False)
            )
    })
    return Output(data, metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Fill the missing description data for each FIGI.",
    group_name="Portfolios_Upload",
    ins={
        "data": AssetIn("new_portfolio_columns")
    }
)
def missing_portfolio_values(context: AssetExecutionContext, data: pd.DataFrame) -> Output:

    if len(data) == 0:
        context.log.info(f"No data found")
        return Output(data)

    for security_name, yellow_code in constants.MISSING_YELLOW_CODES.items():
        query = data["security_name"].fillna("").str.upper().str.startswith(security_name)
        data.loc[query, "yellow_key_code"] = yellow_code
        context.log.info(f"Normalized {security_name}S")

    figi_codes = data["figi_code"].dropna().unique().tolist()
    
    get_nan_columns = lambda df: df.columns[(df.isna().sum() > 0).to_frame().T.values[0]]
    for figi_code in figi_codes:

        current = data.loc[data["figi_code"] == figi_code]
        for column in get_nan_columns(current):
    
            values = current[column].dropna().unique()
            if not values:
                continue
            
            data.loc[current.index, column] = values[0]

        context.log.info(f"FIGI ({figi_code}) columns fixed")
    
    return Output(data)



@asset(
    compute_kind="Pandas",
    description="Split the data into consistent and inconsistent.",
    group_name="Portfolios_Upload",
    ins={
        "data": AssetIn("missing_portfolio_values")
    }
)
def filter_portfolios_data(context: AssetExecutionContext, data: pd.DataFrame) -> Output:

    if len(data) == 0:
        context.log.info(f"No data found")
        return Output((pd.DataFrame(), pd.DataFrame()))

    checks = {
        "pool_fund_code": "No Fund Code found.",
        "yellow_key_code": "No Bloomberg Yellow Key Code found.",
        "issuer_country_code": "No country code found.",
        "security_currency": "No security currency found."
    }

    faulty = []

    for column, comment in checks.items():

        nans = data[column].isna()

        if nans.sum() == 0:
            continue
        
        current = data.loc[nans].reset_index(drop=True).copy()
        current.insert(1, "column_name", column)
        current.insert(2, "comment", comment)
        faulty.append(current)
    
    faulty = pd.DataFrame() if not faulty else pd.concat(faulty, ignore_index=True).drop_duplicates().reset_index(drop=True)
    
    query = ~data["id"].isin(faulty["id"])
    consistent = data.loc[query].reset_index(drop=True).copy()
    
    metadata = {
        "total": len(data),
        "consistent": len(consistent),
        "faulty": len(faulty)
    }

    return Output((consistent, faulty), metadata=metadata)



@asset(
    compute_kind="Mongodb",
    description="Upload the consistent portfolio data to the database",
    group_name="Portfolios_Upload",
    ins={
        "data": AssetIn("filter_portfolios_data")
    }
)
def new_portfolio_data(context: AssetExecutionContext, data: tuple[pd.DataFrame, pd.DataFrame], mongo: Mongo) -> MaterializeResult:

    consistent, _ = data
    collection = mongo.processed_portfolio

    metadata = {
        "uploaded": len(consistent) > 0,
        "rows": len(consistent),
        "collection": f"{collection.database.name}/{collection.name}"
    }

    if not metadata["uploaded"]:
        context.log.info(f"No new portfolio data")
        return MaterializeResult(metadata=metadata)
    
    collection.insert_many(consistent.to_dict("records"))
    return MaterializeResult(metadata=metadata)