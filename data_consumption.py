import pymongo
import datetime
import pandas as pd
from typing import Optional, Union

class DataLoader:

    def __init__(self, url: str = "mongodb://localhost:27017/"):
        """
        Class to quickly access prices within the database

        Parameters:
            - `url` - the MongoDB URL that the data will be fetched from
        """
        self.__client = pymongo.MongoClient(url)
        self.__processed_db = self.__client["processed"]

        self.__prices = self.__processed_db["prices"]
        self.__country_map, self.__country_code_map = self.__get_country_mappings()
   


    def __get_country_mappings(self) -> tuple[dict, dict]:
        """
        Get the country code to country name mapping

        Output:
            - the country code (2 digits) to full country name
            - the full country name to the country code (2 digits)
        """
        
        collection = self.__processed_db["country_mappings"]
        project = {
            "_id": 0,
            "country": "$country_name",
            "code": "$country_code"
        }

        data = pd.DataFrame(collection.find({}, project))
        
        return data.set_index("code")["country"].to_dict(), data.set_index("country")["code"].to_dict()



    def __check_date(self, date: Optional[Union[str, datetime.datetime, pd.Timestamp]]) -> Optional[datetime.datetime]:
        """
        Checks if the given date is in either a `str` or `datetime.datetime` format.
        If the `str` input is not in the correct format then raise a custom error
        error with additional information.

        Parameters:
            - `date` - the date to be checked. If the date is a `str` the format must be `%Y-%m-%d`

        Output:
            - either a `datetime` or `None`
        """
        if isinstance(date, type(None)):
            return date
        
        if isinstance(date, str):
            format = "%Y-%m-%d"
            try:
                return datetime.datetime.strptime(date, format)
            except:
                raise Exception(f"Invalid date format! Please make sure the date is in {format} format...")
        
        if isinstance(date, pd.Timestamp):
            return date.to_pydatetime()
        
        if isinstance(date, datetime.datetime):
            return date
        
        return None



    def load(self, bbg_code: Optional[str] = None, 
                   issuer: Optional[str] = None,
                   start_date: Optional[Union[str, datetime.datetime, pd.Timestamp]] = None, 
                   end_date: Optional[Union[str, datetime.datetime, pd.Timestamp]] = None, 
        ) -> pd.DataFrame:
        """
        Fetch the prices from the processed data.

        Parameters:
            - `bbg_code` - the Bloomberg Code as it is in the security master
            - `issuer` - the Issuer Country Code (2 characters) or the Issuer Country as it is in the security master
            - `start_date` - the starting date the prices will be fetched from
            - `start_date` - the ending period the prices will be fetched up to
        
        Output:
            - the filtered out prices
        """

        query = {}

        if isinstance(issuer, str) and len(issuer) == 2 and issuer not in self.__country_map.keys():
            raise Exception("Invalid Country Code...")

        elif isinstance(issuer, str) and len(issuer) > 2 and issuer not in self.__country_code_map.keys():
            raise Exception("Invalid Country name...")
        
        elif isinstance(issuer, str) and len(issuer) < 2:
            raise Exception("Invalid issuer. Please make sure that a valid country code or country name is given")

        start_date = self.__check_date(start_date)
        end_date = self.__check_date(end_date)

        if start_date and end_date and start_date > end_date:
            raise Exception("Start date cannot be greater than end date...")
        
        query = {}
        sub_query = {}
        if start_date:
            sub_query["$gte"] = start_date

        if end_date:
            sub_query["$lte"] = end_date

        if sub_query:
            query["date"] = sub_query

        if bbg_code:
            query["bbg_code"] = bbg_code

        if issuer:
            country_code = issuer if len(issuer) == 2 else self.__country_code_map[issuer]
            query["country_code"] = country_code

        prices = pd.DataFrame(self.__prices.find(query).sort("date", 1))
        if len(prices) > 0:
            prices["country"] = prices["country_code"].map(self.__country_map)
            prices = prices.drop(["_id", "country_code"], axis=1)
        else:
            prices = pd.DataFrame(columns=["date", "bbg_code", "price", "ccy", "country"])
        
        return prices.copy()



    def __get_security_master_info(self, figi_code: Optional[str] = None, bbg_code: Optional[str] = None, issuer: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch instrument information for the given FIGI code / Blooberg Code / Issuer.
        The parameters can be used independently and in different combinations.

        Parameters:
            - `figi_code` - the FIGI code as it is in the security master
            - `bbg_code` - the Bloomberg Code as it is in the security master
            - `issuer` - the Issuer Country Code (2 characters) or the Issuer Country as it is in the security master

        Output:
            - overall information for each found instrument
        """
        query = {}

        if figi_code:
            query["figi_code"] = figi_code

        if bbg_code:
            query["bbg_code"] = bbg_code

        if issuer:
            key = "issuer_country_code" if len(issuer) == 2 else "issuer_country"
            query[key] = issuer

        project = {
            "_id": 0,
            "bbg_code": "$bbg_code",
            "yellow_code": "$yellow_key_code",
            "figi": "$figi_code",
            "security_name": "$security_name",
            "country": "$issuer_country"
        }

        info = pd.DataFrame()
        if query:
            info = pd.DataFrame(self.__security_master.find(query, project))       
        
        return info



if __name__ == "__main__":

    loader = DataLoader()

    params = {
        # "bbg_code": "TSLA UW",
        # "start_date": "2024-01-23",
        # "end_date": "2024-01-31",
        "issuer": "US"
    }
    
    prices = loader.load(**params)

    