from dagster import asset, Output, AssetIn, MarkdownMetadataValue, AssetExecutionContext, MaterializeResult
from .. import constants
from ..resources import Mongo
from ..configs import RawFilesConfig
import pandas as pd
import datetime

@asset(
    compute_kind="Pandas",
    description="Get all of the country names and their correct country code (2 digits)",
    group_name="Country_Codes_Upload",
)
def country_codes(config: RawFilesConfig) -> Output:

    column_names = {
        "Name": "country_name",
        "Code": "country_code"
    }
    # NOTE: Data is coming from https://datahub.io/core/country-list
    # Fixes have been made based on https://nu-res.research.northeastern.edu/wp-content/uploads/2019/04/2_Digit_Country_Codes-4.25.19.pdf
    data = pd.read_csv(config.file_path).drop_duplicates().reset_index(drop=True).rename(columns=column_names)
    
    metadata = {
        "preview": MarkdownMetadataValue(data.head(20).to_markdown(index=False)),
        "countries": len(data)
    }
    return Output(data, metadata=metadata)



@asset(
    compute_kind="Mongodb",
    description="Fetch the existing country codes from the database",
    group_name="Country_Codes_Upload",
)
def existing_country_codes(mongo: Mongo) -> Output:
    
    data =  pd.DataFrame(mongo.country_codes.find())
    if len(data) > 0:
        data = data.drop("_id", axis=1)
    
    metadata = {
        "existing": str(len(data))
    }
    return Output(data, metadata=metadata)



@asset(
    compute_kind="Mongodb",
    description="Get all of the country names and their correct country code (2 digits)",
    group_name="Country_Codes_Upload",
    ins={
        "raw": AssetIn("country_codes"),
        "existing": AssetIn("existing_country_codes")
    }
)
def new_country_codes(raw: pd.DataFrame, existing: pd.DataFrame, mongo: Mongo) -> MaterializeResult:

    metadata = {
        "existing": str(len(existing)),
        "uploaded": len(existing) == 0
    }

    if len(existing) == 0:
        mongo.country_codes.insert_many(raw.to_dict("records"))
        return MaterializeResult(metadata=metadata)

    new = raw.merge(existing, "left", ["country_name", "country_code"], indicator=True).copy()
    new = new.loc[new["_merge"] == "left_only"].reset_index(drop=True)
    metadata["new"] = str(len(new))

    metadata["uploaded"] = len(new) > 0

    if metadata["uploaded"]:
        new = new.drop("_merge", axis=1)
        mongo.country_codes.insert_many(new.to_dict("records"))

    return MaterializeResult(metadata=metadata)