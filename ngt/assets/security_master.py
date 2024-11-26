from dagster import asset, Output, AssetIn, MarkdownMetadataValue, AssetExecutionContext, MaterializeResult
from .. import constants
from ..resources import Mongo
import pandas as pd
import datetime

@asset(
    compute_kind="Pandas",
    description="Create the portfolio instruments that will be uploaded to the security master",
    group_name="Security_Master_Upload",
    ins={
        "data": AssetIn("uploaded_raw_portfolios")
    }
)
def portfolio_instruments_rename(data: pd.DataFrame) -> Output:

    column_mappings = {
        "nt_security_name": "security_name",
        "nt_yellow_key_code": "yellow_key_code",
        "nt_underlying_security_name": "underlying_security_name",
        "nt_security_currency": "ccy",
        "nt_figi_code": "figi_code",
        "nt_bloomberg_code": "bbg_code",
        "nt_bloomberg_code_of_underlying": "underlying_bbg_code",
        "nt_gti_code": "gti_code",
        "nt_second_quotation_currency": "second_quotation_ccy",
        "nt_issuer_country_code": "issuer_country_code"
    }
    
    data = data.loc[data["nt_quantity"] != 0].reset_index(drop=True)
    instruments = data[list(column_mappings.keys())]\
                    .rename(columns=column_mappings)\
                    .drop_duplicates().copy()

    metadata = {
        "rows": len(instruments)
    }

    return Output(instruments, metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Process the new portfolio instruments for the security master",
    group_name="Security_Master_Upload",
    ins={
        "instruments": AssetIn("portfolio_instruments_rename")
    }
)
def processed_portfolio(context: AssetExecutionContext, instruments: pd.DataFrame) -> Output:

    metadata = {
        "rows": len(instruments)
    }

    if len(instruments) == 0:
        return Output(instruments, metadata=metadata)

    # (1) Equities have the same underlying security name + bbg_code
    query = instruments["yellow_key_code"].isin(["Equity"])
    instruments.loc[query, "underlying_security_name"] = instruments.loc[query, "security_name"]
    instruments.loc[query, "underlying_bbg_code"] = instruments.loc[query, "bbg_code"]    
    context.log.info(f"Normalized Equities")
    
    # (2) Fill missing Bloomberg Yellow Codes
    for security_name, yellow_code in constants.MISSING_YELLOW_CODES.items():
        query = instruments["security_name"].fillna("").str.upper().str.startswith(security_name)
        instruments.loc[query, "yellow_key_code"] = yellow_code
        metadata[security_name] = int(query.sum())
        context.log.info(f"Normalized {security_name}S")

    metadata["preview"] = MarkdownMetadataValue(instruments.head(20).to_markdown(index=False))
    
    return Output(instruments, metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Remove all FIGI duplicates",
    group_name="Security_Master_Upload",
    ins={
        "instruments": AssetIn("processed_portfolio")
    }
)
def unique_instruments(context: AssetExecutionContext, instruments: pd.DataFrame) -> Output:

    if len(instruments) == 0:
        return Output(instruments)

    figi_count = instruments.assign(count=1).groupby("figi_code").sum(["count"]).sort_values("count", ascending=False).reset_index().copy()
    figi_duplicates = figi_count.loc[figi_count["count"] > 1, "figi_code"].to_list()
    
    duplicated = {}

    for figi_code in figi_duplicates:

        query = instruments["figi_code"] == figi_code
        current: pd.DataFrame = instruments.loc[query].reset_index(drop=True).copy()

        # Get the FIGI with the lowest number of NaNs
        min_index = current.isna().sum(axis=1).idxmin()
        duplicated[figi_code] = current.iloc[min_index].to_frame().T.reset_index(drop=True).copy()
        context.log.info(f"Removed duplicates for {figi_code}")

    instruments = instruments.loc[~instruments["figi_code"].isin(list(duplicated.keys()))].reset_index(drop=True)
    instruments = pd.concat([instruments, *list(duplicated.values())], ignore_index=True).dropna(subset="yellow_key_code")

    metadata = {
        "duplicates": len(figi_duplicates)
    }
    return Output(instruments, metadata=metadata)



@asset(
    compute_kind="Pandas",
    description="Assign a country name for every security",
    group_name="Security_Master_Upload",
)
def portfolio_security_master(unique_instruments: pd.DataFrame, country_codes: pd.DataFrame) -> Output:

    metadata = {
        "uploaded": len(unique_instruments) > 0,
        "rows": len(unique_instruments)
    }
    if len(unique_instruments) == 0:
        return Output(pd.DataFrame(), metadata=metadata)

    data = unique_instruments.merge(country_codes, "left", "issuer_country_code").copy()

    metadata["preview"] = MarkdownMetadataValue(data.head(20).to_markdown(index=False))
    
    return Output(data, metadata=metadata)



@asset(
    compute_kind="Mongodb",
    description="Assign a country name for every security",
    group_name="Security_Master_Upload",
)
def new_securities(context: AssetExecutionContext, portfolio_security_master: pd.DataFrame, mongo: Mongo) -> Output:

    collection = mongo.security_master
    new_secs = []
    
    for row in portfolio_security_master.to_dict("records"):

        row = {key: value if pd.notna(value) else None for key, value in row.items()}
        
        results = collection.count_documents(row)
        
        if results > 0:
            continue
        
        context.log.info(f"Added {row}")
        row["upload_timestamp"] = datetime.datetime.now()
        new_secs.append(row)
        
    if new_secs:
        context.log.info(f"Added {len(new_secs)} row(s)")
        collection.insert_many(new_secs)

    metadata = {
        "uploaded": len(new_secs) > 0,
        "rows": len(new_secs),
        "collection": f"{collection.database.name}/{collection.name}"
    }

    return MaterializeResult(metadata=metadata)