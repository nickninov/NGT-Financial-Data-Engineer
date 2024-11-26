from dagster import Config
from pydantic import Field
from typing import Union, Optional

class RawFilesConfig(Config):
    file_path: str = Field(description="The full file path of the portfolio file")

class Figi(Config):
    code: str = Field(description="The figi that will be looked up")
    ccy: Optional[str] = Field(description="The figi's currency that will be used as part of the search")

class FigiConfig(Config):
    figis: list[Figi] = Field(default=[], description="The figis that will be used to fetch additional data for the security master")

class EmailConfig(Config):
    to: list[str] = Field(description="The TO emails")
    cc: Optional[list[str]] = Field(default=None, description="The CC emails")
    file_path: Optional[str] = Field(default=None, description="The file that will be attached")