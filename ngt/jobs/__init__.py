from dagster import define_asset_job, AssetSelection, RunConfig, EnvVar
from ..configs import RawFilesConfig, EmailConfig
from .. import resources
from .. import constants
import os

portfolio_upload_job = define_asset_job(
    name="portfolio_upload_job",
    selection=AssetSelection.groups("Portfolios_Upload") | AssetSelection.groups("Security_Master_Upload") | AssetSelection.groups("Country_Codes_Upload") | AssetSelection.assets("figi_queue", "inconsistent_portfolios_email", "inconsistent_portfolios_data"),
    config=RunConfig(ops={
        "portfolios_file_data": RawFilesConfig(
            file_path=EnvVar("PORTFOLIO_FILE_PATH")
        ),
        "country_codes": RawFilesConfig(
            file_path=EnvVar("COUNTRY_CODE_FILE_PATH")
        ),
        "inconsistent_portfolios_email": EmailConfig(
            to=constants.EMAILS["to"], 
            file_path=os.path.join(constants.HITL_PATH, "Faulty Portfolios.xlsx")
        ),
    },
    resources={
        "mongo": resources.Mongo(url=EnvVar("MONGO_URL")),
        "mail": resources.Email(
            sender=EnvVar("EMAIL_SENDER"), 
            host=EnvVar("EMAIL_HOST"), 
            port=EnvVar("EMAIL_PORT"), 
            username=EnvVar("EMAIL_USERNAME"), 
            password=EnvVar("EMAIL_PASSWORD")
        )
    })
)

trades_upload_job = define_asset_job(
    name="trades_upload_job",
    selection=AssetSelection.groups("Trades_Upload") | AssetSelection.groups("Price_Upload") | AssetSelection.assets("inconsistent_trades_email", "inconsistent_trades_data"),
    config=RunConfig(ops={
        "trades_file_data": RawFilesConfig(file_path=EnvVar("TRADES_FILE_PATH")),
        "inconsistent_trades_email": EmailConfig(
            to=constants.EMAILS["to"],
            file_path=os.path.join(constants.HITL_PATH, "Faulty Trades.xlsx")
        ),
    },
    resources={
        "mongo": resources.Mongo(url=EnvVar("MONGO_URL")),
        "mail": resources.Email(
            sender=EnvVar("EMAIL_SENDER"), 
            host=EnvVar("EMAIL_HOST"), 
            port=EnvVar("EMAIL_PORT"), 
            username=EnvVar("EMAIL_USERNAME"), 
            password=EnvVar("EMAIL_PASSWORD")
        )
    })
)

open_figi_download_job = define_asset_job(
    name="open_figi_download_job",
    selection=["new_figis"]
)

security_master_figi_update_job = define_asset_job(
    name="security_master_figi_update_job",
    selection=["figi_security_master"]
)

upload_fixed_portfolio_data = define_asset_job(
    name="upload_fixed_portfolio_data",
    selection=["fixed_inconsistent_portfolio_data"]
)