from dagster import ConfigurableResource
from typing import Optional, Union
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib
import os
import pandas as pd
import requests
import pymongo
import pymongo.collection

class Mongo(ConfigurableResource):

    url: str

    __client: Optional[pymongo.collection.Collection] = None

    def connect(self) -> pymongo.MongoClient:
        if self.__client:
            return self.__client
        
        self.__client = pymongo.MongoClient(self.url)
        return self.__client

    @property
    def raw_portfolio(self) -> pymongo.collection.Collection:
        return self.connect()["raw"]["portfolio"]
    
    @property
    def raw_trades(self) -> pymongo.collection.Collection:
        return self.connect()["raw"]["trades"]
    
    @property
    def processed_portfolio(self) -> pymongo.collection.Collection:
        return self.connect()["processed"]["portfolio"]
    
    @property
    def inconsistent_portfolio(self) -> pymongo.collection.Collection:
        return self.connect()["faulty"]["portfolio"]

    @property
    def processed_trades(self) -> pymongo.collection.Collection:
        return self.connect()["processed"]["trades"]

    @property
    def inconsistent_trades(self) -> pymongo.collection.Collection:
        return self.connect()["faulty"]["trades"]

    @property
    def open_figi(self) -> pymongo.collection.Collection:
        return self.connect()["api"]["open_figi"]
    
    @property
    def figi_queue(self) -> pymongo.collection.Collection:
        return self.connect()["raw"]["figi_queue"]

    @property
    def security_master(self) -> pymongo.collection.Collection:
        return self.connect()["processed"]["security_master"]
    
    @property
    def prices(self) -> pymongo.collection.Collection:
        return self.connect()["processed"]["prices"]
    
    @property
    def country_codes(self) -> pymongo.collection.Collection:
        return self.connect()["processed"]["country_mappings"]
    
    

class OpenFigi(ConfigurableResource):
    
    api_key: str
    MAX_REQUESTS_PER_MINUTE: int = 20 # As stated on the website with API key
    __session: Optional[requests.Session] = None
    __api_url: str = "https://api.openfigi.com/v3/"

    @property
    def session(self):
        """
        Create a OpenFigi authenticated session
        """
        if self.__session:
            return self.__session

        headers = {
            'Content-Type': 'Application/json',
            'X-OPENFIGI-APIKEY': self.api_key
        }

        session = requests.Session()
        session.headers.update(headers)

        self.__session = session
        return self.__session



    def search(self, search: str, ccy: Optional[str] = None) -> pd.DataFrame:
        """
        Use the OpenFigi v3 Search functionality
        https://www.openfigi.com/api#post-v3-search

        Parameters:
            - `search` - Key words
            - `ccy` - Currency associated to the desired instrument(s)

        Output:
            - all of the fetched `data` for the given figi and currency (optional)
        """
        
        url = self.__api_url + "search"

        query = {"query": search}
        if ccy:
            query["currency"] = ccy
        
        data = []
        
        while True:

            response = self.session.post(url, json=query)
            local_json = dict(response.json())

            data += local_json["data"]
            if "next" not in local_json.keys():
                break
            
            query["start"] = local_json["next"]
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    


class Email(ConfigurableResource):

    sender: str
    host: str
    port: str
    username: str
    password: str

    def send(self, to: Union[list[str], str], subject: str, body: str, cc: Optional[Union[list[str], str]] = None, attachment_paths: Optional[Union[list[str], str]] = None):
        """
        Send an email

        Parameters:
            - `to` - the TO email addresses
            - `subject` - the email's subject
            - `body` - the email's body. Can be either HTML or plain text
            - `cc` - the CC email addresses
            - `attachment_paths` - the attachment(s) that will be sent with the email
        """
        message = MIMEMultipart()

        if isinstance(to, str):
            to = [to]
        
        message["To"] = "; ".join(to)
        if cc:
            message["CC"] = "; ".join(cc if isinstance(cc, list) else [cc])
        
        if attachment_paths:
            
            attachment_paths = [attachment_paths] if isinstance(attachment_paths, str) else attachment_paths

            for path in attachment_paths:
                
                file_name = os.path.basename(path)
                
                with open(path, "rb") as file:
                    part = MIMEApplication(file.read(), Name = file_name)
                    part["Content-Disposition"] = f"attachment; filename=\"{str(file_name)}\""
                    message.attach(part)

        message["Subject"] = subject

        body_type = "html" if body.find("<html>") != -1 or body.find("</") != -1 else "plain"
        message.attach(MIMEText(body, body_type))

        server = smtplib.SMTP_SSL(host=self.host, port=int(self.port))
        server.login(self.username, self.password)

        server.sendmail(self.sender, to, message.as_string())
