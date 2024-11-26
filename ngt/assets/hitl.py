from dagster import asset, Output, AssetExecutionContext, AssetIn, MarkdownMetadataValue, MaterializeResult
from ..resources import Mongo
from ..configs import RawFilesConfig
import pandas as pd
import os
import shutil
import datetime


@asset(
    compute_kind="Mongodb",
    description="Upload the fixed inconsistent portfolio rows",
    group_name="Human_In_The_Loop",
    deps=["inconsistent_portfolios_data"]
)
def fixed_inconsistent_portfolio_data(context: AssetExecutionContext, config: RawFilesConfig, mongo: Mongo) -> Output:

    data = pd.read_excel(config.file_path)

    metadata = {
        "rows": 0
    }

    completed = []

    for id, group in data.groupby("id"):

        group = group.drop(["column_name", "comment"], axis=1).ffill().bfill().drop_duplicates().reset_index(drop=True)
        group["upload_timestamp"] = datetime.datetime.now()
        completed.append(group.copy())

        metadata["rows"] += len(group)

        mongo.processed_portfolio.insert_many(group.to_dict("records"))

        context.log.info(f"Uploaded {id}")
        query = {"id": id}
        update = {
            "$set": {
                "completed_timestamp": datetime.datetime.now(),
                "status": "completed"
            }
        }
        mongo.inconsistent_portfolio.update_many(query, update)

    completed_file_path = os.path.join(os.path.dirname(config.file_path), "completed", os.path.basename(config.file_path))
    os.makedirs(os.path.dirname(completed_file_path), exist_ok=True)

    shutil.move(config.file_path, completed_file_path)
    context.log.info("File was archived")

    completed = pd.concat(completed, ignore_index=True) if completed else pd.DataFrame()
    if len(completed) > 0:
        completed["preview"] = MarkdownMetadataValue(completed.to_markdown(index=False))
    
    return Output(completed, metadata=metadata)