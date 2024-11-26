from dagster import asset, Output, AssetExecutionContext, AssetIn, MarkdownMetadataValue, MaterializeResult
from ..resources import Mongo
from ..configs import RawFilesConfig
from .. import constants
import numpy as np
import pandas as pd
import os
import datetime

@asset(
    compute_kind="Pandas",
    description="Load the trades data into a DataFrame.",
    group_name="Trades_Upload"
)
def trades_file_data(context: AssetExecutionContext, config: RawFilesConfig) -> Output:
    
    is_csv = config.file_path.endswith(".csv")
    data = pd.read_csv(config.file_path) if is_csv else pd.read_excel(config.file_path)

    context.log.info(f"Opened file {os.path.basename(config.file_path)}")

    initial_rows = len(data)
    data = data.dropna(subset="nt_trade_date").drop_duplicates()
    context.log.info(f"Dropped duplicates (from {initial_rows} to {len(data)})")

    metadata = {
        "rows": len(data),
        "preview": MarkdownMetadataValue(data.head(20).to_markdown(index=False)),
        "file_name": os.path.basename(config.file_path),
        "fund_codes": "; ".join(data["nt_fund_code"].unique())
    }

    return Output(data, metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Filter out the trades that have a price or quantity 0 / NaN. If a trade has the date but a different price, the trades are filtered out as well.",
    group_name="Trades_Upload",
    ins={
        "data": AssetIn("trades_file_data")
    }
)
def filter_trades_data(context: AssetExecutionContext, data: pd.DataFrame) -> Output:

    # Price / Quantity is NaN or 0
    query = data["nt_transaction_quantity"].isna() | data["nt_transaction_price"].isna() | (data["nt_transaction_quantity"] == 0) | (data["nt_transaction_price"] == 0)

    metadata = {
        "total": len(data)
    }
    missing = data.loc[query].drop_duplicates().reset_index(drop=True).copy()
    data = data.loc[~query].drop_duplicates().reset_index(drop=True).copy()

    # Price for the same date is different
    index_duplicates = []
    for (bbg_code, ccy), group in data.groupby(["nt_bloomberg_code", "nt_security_currency"]):
        transactions = group[["nt_trade_date", "nt_security_currency", "nt_transaction_price"]].copy()

        duplicates = transactions["nt_trade_date"].duplicated(keep=False) & (~transactions["nt_transaction_price"].duplicated(keep=False))
        if duplicates.sum() == 0:
            continue
        
        context.log.info(transactions.loc[duplicates].to_string(index=False))
        index_duplicates += transactions.loc[duplicates].index.to_list()
    
    if index_duplicates:
        index_duplicates = list(set(index_duplicates))
        missing = pd.concat([missing, data.iloc[index_duplicates]], ignore_index=True).drop_duplicates()

    data = data.drop(index=index_duplicates).drop_duplicates().reset_index(drop=True)
    metadata.update({
        "missing": len(missing),
        "data": len(data),
    })

    return Output((data, missing), metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Prepare the raw data to be uploaded.",
    group_name="Trades_Upload",
    ins={
        "trades": AssetIn("filter_trades_data")
    }
)
def trades_raw_processed_data(context: AssetExecutionContext, trades: tuple[pd.DataFrame, pd.DataFrame]) -> Output:

    data, _ = trades

    if len(data) == 0:
        context.log.info("No data found...")
        return Output(pd.DataFrame())
    
    data = data.drop_duplicates().reset_index(drop=True)

    data = data.assign(
        nx_date = pd.to_datetime(data["nx_date"], format="%m/%d/%Y"),
        nt_accounting_date = pd.to_datetime(data["nx_date"], format="%m/%d/%Y"),
        nt_trade_date = pd.to_datetime(data["nt_trade_date"], format="%m/%d/%Y %H:%M:%S")
    )
    context.log.info("Fixed date column")

    id = data["nt_trade_date"].dt.strftime("%Y-%m-%d") + "/" + data["nt_accounting_date"].dt.strftime("%Y-%m-%d") + "/"+ data["nt_fund_code"] + "/" + data["nt_gti_code"] + "/" + data["nt_security_description"].str.replace(" ", "_") + "/" + data["nt_security_currency"] + "/" + data["nt_transaction_price"].astype(str) + "(" + data["nt_transaction_quantity"].astype(str).str.replace("-", "NEG") + ")"
    data.insert(0, "id", id)
    context.log.info("Created unique ID")

    data["upload_timestamp"] = datetime.datetime.now()
    metadata = {
        "preview": MarkdownMetadataValue(data.head(20).to_markdown(index=False))
    }

    return Output(data, metadata=metadata)


@asset(
    compute_kind="Pandas",
    description="Prepare the raw data to be uploaded.",
    group_name="Trades_Upload",
    ins={
        "data": AssetIn("uploaded_raw_trades")
    }
)
def new_trades_columns(context: AssetExecutionContext, data: pd.DataFrame) -> Output:

    if len(data) == 0:
        context.log.info("No data found...")
        return Output(pd.DataFrame())
    
    data.columns = [column[3:] if column.startswith("nt_") else column for column in data.columns.to_list()]
    data = data.drop(["nx_date"], axis=1)
    
    data["notional_value"] = data["transaction_quantity"] * data["transaction_price"]
    context.log.info("Notional Value calculated")

    data["long_short"] = "long"
    data.loc[data["transaction_quantity"] < 0, "long_short"] = "short"
    context.log.info("Added long/short")

    data["issuer_country_code"] = data["issuer_country_name"].map(constants.TRADES_COUNTRY_MAPPING)
    context.log.info("Added country code")

    columns = ["trade_date", "bloomberg_code", "transaction_price"]
    prices = []
    
    for bbg_code, group in data.groupby("bloomberg_code"):

        start_date = group["trade_date"].min()
        end_date = group["trade_date"].max()

        historical = pd.bdate_range(start_date, end_date)\
                    .to_frame(name="trade_date")\
                    .reset_index(drop=True)\
                    .merge(group, "left", "trade_date")[columns]\
                    .drop_duplicates()\
                    .copy()
        
        historical = historical.assign(
            pct_change = historical["transaction_price"].fillna(0).pct_change().apply(lambda value: None if value == np.inf else value)
        ).dropna(subset="bloomberg_code")\
        .reset_index(drop=True)
        
        prices.append(historical.copy())

    prices = pd.concat(prices, ignore_index=True)
    data = data.merge(prices, "left", columns)
    context.log.info("Price change has been calculated")
    
    return Output(data)



@asset(
    compute_kind="Pandas",
    description="Prepare the raw data to be uploaded.",
    group_name="Trades_Upload",
    ins={
        "data": AssetIn("new_trades_columns")
    }
)
def new_processed_trades(context: AssetExecutionContext, data: pd.DataFrame, mongo: Mongo) -> MaterializeResult:

    collection = mongo.processed_trades

    metadata = {
        "uploaded": len(data) > 0,
        "rows": len(data),
        "collection": f"{collection.database.name}/{collection.name}"
    }
    if len(data) == 0:
        context.log.info("No data found...")
        return MaterializeResult(metadata=metadata)
    
    collection.insert_many(data.to_dict("records"))
    context.log.info(f"Uploaded {len(data)} trade(s)")
    return MaterializeResult(metadata=metadata)



@asset(
    compute_kind="Mongodb",
    description="Prepare the raw data to be uploaded.",
    group_name="Trades_Upload",
    ins={
        "trades": AssetIn("new_trades_columns")
    }
)
def new_prices(context: AssetExecutionContext, trades: pd.DataFrame, mongo: Mongo) -> MaterializeResult:

    collection = mongo.prices

    metadata = {
        "uploaded": len(trades) > 0,
        "collection": f"{collection.database.name}/{collection.name}",
        "rows": len(trades),
    }
    if not metadata["uploaded"]:
        context.log.info("No data found...")
        return MaterializeResult(metadata=metadata)
    
    column_mappings = {
        "trade_date": "date",
        "bloomberg_code": "bbg_code",
        "transaction_price": "price",
        "security_currency": "ccy",
        "issuer_country_code": "country_code"
    }

    trades = trades[list(column_mappings.keys())]\
                    .rename(columns=column_mappings)\
                    .drop_duplicates()\
                    .reset_index(drop=True)
    
    collection.insert_many(trades.to_dict("records"))
    context.log.info(f"Uploaded {len(trades)} trade(s)")

    return MaterializeResult(metadata=metadata)