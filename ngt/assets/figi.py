from dagster import asset, Output, AssetExecutionContext, AssetIn, MaterializeResult
from ..configs import FigiConfig
from ..resources import OpenFigi, Mongo
import pandas as pd
import time
import datetime


@asset(
    compute_kind="Mongodb",
    description="Fetch the information about the FIGIs",
    group_name="Figi_Upload",
    ins={
        "data": AssetIn("uploaded_raw_portfolios")
    }
)
def figi_queue(context: AssetExecutionContext, data: pd.DataFrame, mongo: Mongo) -> MaterializeResult:

    if len(data) == 0:
        return MaterializeResult()
    
    figis = data[["nt_figi_code", "nt_security_currency", "upload_timestamp"]]\
                    .dropna().drop_duplicates().copy()
    
    queued = mongo.figi_queue.distinct("nt_figi_code")

    query = ~figis["nt_figi_code"].isin(queued)

    if query.sum() > 0:
        mongo.figi_queue.insert_many(figis.loc[query].to_dict("records"))
        context.log.info(f"Added {len(figis)} to the OpenFIGI API queue")
    else:
        context.log.info("No new FIGIs uploaded")
    
    metadata = {
        "total": len(figis),
        "new": int(query.sum())
    }

    return MaterializeResult(metadata=metadata)



@asset(
    compute_kind="OpenFIGI API",
    description="Fetch the FIGI information about the FIGIs",
    group_name="Figi_Upload",
    deps = ["figi_queue"]
)
def new_figis(context: AssetExecutionContext, open_figi: OpenFigi, mongo: Mongo, config: FigiConfig) -> MaterializeResult:
    
    collection = mongo.open_figi
    metadata = {
        "uploaded": 0,
        "skipped": 0,
        "collection": f"{collection.database.name}/{collection.name}"
    }

    requests = 1
    
    for figi in config.figis:
        
        if requests >= open_figi.MAX_REQUESTS_PER_MINUTE:
            context.log.info(f"Waiting 1 minute for OpenFIGI requests to refresh")
            time.sleep(70) # Sleep for slightly longer just in case of server delay
            requests = 0

        context.log.info(f"Starting {figi.code}")

        data = open_figi.search(figi.code, figi.ccy)
        requests += 1

        query = {"nt_figi_code": figi.code}
        update = {
            "$set": {
                "completed_timestamp": datetime.datetime.now(),
                "found": len(data) > 0
            }
        }
        mongo.figi_queue.update_many(query, update)

        if len(data) == 0:
            context.log.warning(f"No data found for {figi.code}")
            metadata["skipped"] += 1
            continue

        data = data.assign(
            ccy = figi.ccy,
            upload_timestamp=datetime.datetime.now(),
        )

        context.log.info(f"Uploaded {figi.code} API data")
        collection.insert_many(data.to_dict("records"))
        metadata["uploaded"] += 1

    return MaterializeResult(metadata=metadata)



@asset(
    compute_kind="Mongodb",
    description="Add the FIGI info from OpenFIGI API to the security master",
    group_name="Figi_Upload",
    deps=["new_figis"]
)
def figi_security_master(context: AssetExecutionContext, mongo: Mongo, config: FigiConfig) -> MaterializeResult:

    metadata = {
        "updates": 0,
        "skips": 0
    }
    for figi in config.figis:

        now = datetime.datetime.now()
        query = {
            "figi": figi.code,
            "ccy": figi.ccy
        }

        api_data = mongo.open_figi.find_one(query)
        
        mongo.figi_queue.update_many({
            "nt_figi_code": figi.code,
            "nt_security_currency": figi.ccy
        }, {
            "$set": {
                "security_master_timestamp": now
            }
        })
        if not api_data:
            metadata["skips"] += 1
            continue

        query["figi_code"] = query.pop("figi")        

        security: dict = mongo.security_master.find_one(query)        
        security.pop("upload_timestamp")

        if not security.get("security_name"):
            security["security_name"] = api_data["ticker"]

        security["security_type"] = api_data["securityType"]
        security["yellow_key_code"] = api_data["marketSector"]
        security["security_type_2"] = api_data["securityType2"]
        security["upload_timestamp"] = now

        mongo.security_master.update_one({"_id": security["_id"]}, {"$set": security})
        metadata["updates"] += 1

    return MaterializeResult(metadata=metadata)