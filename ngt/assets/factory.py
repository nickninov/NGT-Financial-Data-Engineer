from dagster import asset, Output, MaterializeResult, AssetExecutionContext, AssetIn, MarkdownMetadataValue
from ..resources import Mongo, Email
from ..configs import EmailConfig
import pandas as pd
import os
import datetime

def make_upload(mode: str) -> Output:

    @asset(
        compute_kind="Mongodb",
        name=f"uploaded_raw_{mode}",
        description=f"Filter out the already uploaded {mode} rows.",
        group_name=f"{mode.title()}_Upload",
        ins={
            "data": AssetIn(f"{mode}_raw_processed_data")
        }
    )
    def asset_template(context: AssetExecutionContext, data: pd.DataFrame, mongo: Mongo) -> Output:

        collection = mongo.raw_portfolio if mode == "portfolios" else mongo.raw_trades
        query = {
            "id": {"$in": data["id"].drop_duplicates().to_list()}
        }
        project = {
            "_id": 0,
            "id": 1
        }
        uploaded_ids = pd.DataFrame(collection.find(query, project))
        uploaded_ids = [] if len(uploaded_ids) == 0 else uploaded_ids["id"].to_list()
        
        context.log.info(f"Found {len(uploaded_ids)}")
        query = data["id"].isin(uploaded_ids)

        metadata = {
            "total": len(data),
            "found": int(query.sum()),
            "new": int(len(data) - query.sum()),
            "collection": f"{collection.database.name}/{collection.name}"
        }

        data = data.loc[~query].reset_index(drop=True)
        return Output(data, metadata=metadata)

    return asset_template



def make_new_data(mode: str) -> Output:

    @asset(
        compute_kind="Mongodb",
        description=f"Upload the new raw {mode}.",
        name=f"new_raw_{mode}_data",
        group_name=f"{mode.title()}_Upload",
        ins={
            "data": AssetIn(f"uploaded_raw_{mode}")
        }
    )
    def asset_template(context: AssetExecutionContext, data: pd.DataFrame, mongo: Mongo) -> Output:

        is_portfolio = mode == "portfolios"
        collection = mongo.raw_portfolio if is_portfolio else mongo.raw_trades

        metadata = {
            "uploaded": len(data) > 0,
            "rows": len(data),
            "preview": "",
            "collection": f"{collection.database.name}/{collection.name}"
        }

        if metadata["uploaded"]:
            metadata["preview"] = MarkdownMetadataValue(data.head(10).to_markdown(index=False))
            collection.insert_many(data.to_dict("records"))
            context.log.info(f"Uploaded {len(data)} row(s)")
        else:
            context.log.warning("No new rows have been uploaded")
            metadata.pop("preview")

        return Output(data, metadata=metadata)
    

    return asset_template



def make_inconsistent_email(mode: str):

    @asset(
        name=f"inconsistent_{mode}_email",
        compute_kind="Python",
        description="Send an alert that there are rows that need to be fixed",
        group_name="Human_In_The_Loop",
        ins={
            "data": AssetIn(f"filter_{mode}_data")
        }
    )
    def asset_template(context: AssetExecutionContext, data: tuple[pd.DataFrame, pd.DataFrame], mail: Email, config: EmailConfig) -> Output:

        _, faulty = data

        metadata = {
            "email": len(faulty) > 0,
            "to": "; ".join(config.to)
        }

        if not metadata["email"]:
            context.log.info("No inconsistent rows to be emailed.")
            return Output(faulty, metadata=metadata)
        
        subject = f"Faulty {mode.title()}"
        body = "<p>The attached file has rows that must be filled up manually. Please further investigate</p>"

        folder = os.path.dirname(config.file_path)
        os.makedirs(folder, exist_ok=True)
        context.log.info(f"Folder {folder} created")

        faulty.to_excel(config.file_path, index=False)
        context.log.info(f"File has been saved - {config.file_path}")

        mail.send(config.to, subject, body, config.cc, config.file_path)
        context.log.info(f"Email was sent to {config.to}")

        return Output(faulty, metadata=metadata)
    
    return asset_template



def make_inconsistent_data(mode: str):
    
    @asset(
        name=f"inconsistent_{mode}_data",
        compute_kind="Mongodb",
        description="Send an alert that there are rows that need to be fixed",
        group_name="Human_In_The_Loop",
        ins={
            "faulty": AssetIn(f"inconsistent_{mode}_email")
        }
    )
    def asset_template(context: AssetExecutionContext, faulty: pd.DataFrame, mongo: Mongo) -> MaterializeResult:

        collection = mongo.inconsistent_portfolio if mode == "portfolios" else mongo.inconsistent_trades
        metadata = {
            "uploaded": len(faulty) > 0,
            "rows": len(faulty),
            "collection": f"{collection.database.name}/{collection.name}"
        }

        if not metadata["uploaded"]:
            context.log.info("No inconsistent rows to be uploaded.")
            return MaterializeResult(metadata=metadata)

        faulty.insert(3, "status", "pending")
        faulty["upload_timestamp"] = datetime.datetime.now()
        faulty["completed_timestamp"] = None

        documents = faulty.to_dict("records")
        collection.insert_many(documents)
        context.log.info(f"Uploaded {len(documents)}")

        return MaterializeResult(metadata=metadata)

    return asset_template