from dagster import sensor, RunRequest, SkipReason, DefaultSensorStatus
from ..resources import Mongo, OpenFigi
from .. import jobs
from .. import constants
import datetime
import os
import pandas as pd

@sensor(
    job = jobs.open_figi_download_job,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=75 # Every 1 min and 15 sec
)
def open_figi_api_sensor(mongo: Mongo, open_figi: OpenFigi):

    collection = mongo.figi_queue

    query = {
        "completed_timestamp": None,   
    }
    project = {
        "_id": 0,
        "code": "$nt_figi_code",
        "ccy": "$nt_security_currency"
    }

    # We do one sample below the MAX limit so that we can ignore the automatic sleep time
    figis = list(collection.find(query, project).limit(open_figi.MAX_REQUESTS_PER_MINUTE-1))
    if not figis:
        return SkipReason("No new Figis to fetch from OpenFIGI API")
    
    run_config = {
        "ops": {
            "new_figis": {
                "config": {
                    "figis": figis
                }
            }
        }
    }

    # Unique key for every run
    run_key = str(int(datetime.datetime.now().timestamp()))

    return RunRequest(run_key, run_config)



@sensor(
    job = jobs.security_master_figi_update_job,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds = 60 * 5 # Every 5 min
)
def security_master_figi_sensor(mongo: Mongo):

    query = {
        "completed_timestamp": {"$ne": None},
        "security_master_timestamp": None,
        "found": True
    }

    project = {
        "_id": 0,
        "code": "$nt_figi_code",
        "ccy": "$nt_security_currency"
    }

    collection = mongo.figi_queue
    figis = pd.DataFrame(collection.find(query, project)).drop_duplicates().to_dict("records")

    if not figis:
        return SkipReason("No new Figis to fetch from OpenFIGI API")
    
    run_config = {
        "ops": {
            "figi_security_master": {
                "config": {
                    "figis": figis
                }
            }
        }
    }

    # Unique key for every run
    run_key = str(int(datetime.datetime.now().timestamp()))
    return RunRequest(run_key, run_config)



@sensor(
    job = jobs.upload_fixed_portfolio_data,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=60 # Every 1 min
)
def fixed_portfolio_data_sensor():

    path = os.path.join(constants.HITL_PATH, "upload", "portfolios")
    os.makedirs(path, exist_ok=True)

    for file_name in os.listdir(path):
        
        if file_name.startswith("~$"):
            continue

        file_path = os.path.join(path, file_name)
        if os.path.isdir(file_path):
            continue
        
        config = {
            "run_key": str(int(datetime.datetime.now().timestamp())),
            "run_config": {
                "ops": {
                    "fixed_inconsistent_portfolio_data": {
                        "config": {
                            "file_path": file_path
                        }
                    }
                }
            }
        }

        yield RunRequest(**config)



@sensor(
    job = jobs.trades_upload_job,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=60 # Every 1 min
)
def reupload_faulty_trades():

    path = os.path.join(constants.HITL_PATH, "upload", "trades")
    os.makedirs(path, exist_ok=True)

    for file_name in os.listdir(path):
        
        if file_name.startswith("~$"):
            continue

        file_path = os.path.join(path, file_name)
        if os.path.isdir(file_path):
            continue

        config = {
            "run_key": str(int(datetime.datetime.now().timestamp())),
            "run_config": {
                "ops": {
                    "trades_file_data": {
                        "config": {
                            "file_path": file_path
                        }
                    },
                    "inconsistent_trades_email": {
                        "config": {
                            "to": constants.EMAILS["to"],
                            "file_path": os.path.join(constants.HITL_PATH, file_name)
                        }
                    }
                }
            }
        }

        yield RunRequest(**config)