from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, EnvVar
from .assets import portfolios, trades, factory, figi, security_master, hitl, country_codes
from . import resources
from . import jobs
from . import sensors

module_assets = load_assets_from_modules([
    portfolios, trades, figi, security_master, hitl, country_codes
])

module_asset_checks = load_asset_checks_from_modules([
    # put asset check modules here
])

defs = Definitions(
    assets = [
        *module_assets, 
        factory.make_upload("portfolios"), factory.make_new_data("portfolios"), 
        factory.make_upload("trades"), factory.make_new_data("trades"),
        factory.make_inconsistent_email("portfolios"), factory.make_inconsistent_email("trades"),
        factory.make_inconsistent_data("portfolios"), factory.make_inconsistent_data("trades")
    ],
    jobs = [
        jobs.portfolio_upload_job, jobs.trades_upload_job
    ],
    sensors = [
        sensors.open_figi_api_sensor, sensors.security_master_figi_sensor,
        sensors.fixed_portfolio_data_sensor, sensors.reupload_faulty_trades
    ],
    schedules = [
        # Insert schedules here. Example schedules.your_schedule_name
    ],
    asset_checks = [*module_asset_checks],
    resources={
        "mongo": resources.Mongo(url=EnvVar("MONGO_URL")),
        "open_figi": resources.OpenFigi(api_key=EnvVar("FIGI_API_KEY")),
        "mail": resources.Email(
            sender=EnvVar("EMAIL_SENDER"), 
            host=EnvVar("EMAIL_HOST"), 
            port=EnvVar("EMAIL_PORT"), 
            username=EnvVar("EMAIL_USERNAME"), 
            password=EnvVar("EMAIL_PASSWORD")
        )
    }
)