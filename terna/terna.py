#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------
# Created By  : fgenoese
# Created Date: Sunday 15 January 2023 at 19:31

import requests
import pandas as pd
import datetime
import time
import logging
from typing import Optional, Dict
from urllib.parse import urlencode

__title__ = "terna-py"
__version__ = "0.2.0b1"
__author__ = "fgenoese"
__license__ = "MIT"

URL = 'https://api.terna.it/transparency/oauth/accessToken'
BASE_URL = 'https://api.terna.it/transparency/v1.0/'

class TernaPandasClient:
    def __init__(
            self, api_key: str, api_secret: str, session: Optional[requests.Session] = None,
            proxies: Optional[Dict] = None, timeout: Optional[int] = None):
        """
        Parameters
        ----------
        api_client : str
        api_secret : str
        session : requests.Session
        proxies : dict
            requests proxies
        timeout : int
        """
        if api_key is None:
            raise TypeError("API key cannot be None")
        if api_secret is None:
            raise TypeError("API secret cannot be None")
        self.api_key = api_key
        self.api_secret = api_secret
        if session is None:
            session = requests.Session()
        self.session = session
        self.proxies = proxies
        self.timeout = timeout

    def _request_token(self, data: Dict = {}) -> str:
        """
        Parameters
        ----------
        data : dict
        
        Returns
        -------
        access_token : str
        """

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        base_data = {
            'client_id': self.api_key,
            'client_secret': self.api_secret,
            'grant_type': 'client_credentials',
        }
        data.update(base_data)

        try:
            response = self.session.post(URL, headers=headers, data=data)
            response.raise_for_status()

        except requests.HTTPError as exc:
            code = exc.response.status_code
            if code in [429, 500, 502, 503, 504]:
                logging.debug(code)
            raise
        
        else:
            if response.status_code == 200:
                time.sleep(1)
                return response.json()['access_token']
            else:
                return None
    
    def _base_request(self, item, data: Dict) -> pd.DataFrame:
        """
        Parameters
        ----------
        data : dict
        
        Returns
        -------
        pd.DataFrame
        """

        access_token = self._request_token()
        data.update({'access_token': access_token})
        params = urlencode(data, doseq=True)
        _url =  "{}{}?{}".format(BASE_URL, item, params)
        logging.debug(_url)
        
        try:
            response = self.session.get(_url)
            response.raise_for_status()

        except requests.HTTPError as exc:
            code = exc.response.status_code
            print(response.text)
            if code in [429, 500, 502, 503, 504]:
                logging.debug(code)
            raise
        else:
            if response.status_code == 200:
                json = response.json()
                if 'result' in json:
                    json.pop('result')
                    key = list(json.keys())[0]
                    df = pd.json_normalize(json[key])
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                        df['Date'] = df['Date'].map(lambda x: adjust_tz(x, tz="Europe/Rome"))
                        df.sort_values(by='Date', inplace=True)
                        df.index = df['Date']
                        df.index.name = None
                        df.drop(columns=['Date'], inplace=True)
                    elif 'Year' in df.columns:
                        df.index = df['Year']
                        df.drop(columns=['Year'], inplace=True)
                    df = df.apply(pd.to_numeric, errors='ignore')
                    return df
                else:
                    return None
            else:
                return None
    
    def get_total_load(self, start: pd.Timestamp, end: pd.Timestamp, bzone: str) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        bzone : str
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
            'biddingZone': bzone
        }
        item = 'gettotalload'

        df = self._base_request(item, data)
        return df

    def get_market_load(self, start: pd.Timestamp, end: pd.Timestamp, bzone: str) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        bzone : str
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
            'biddingZone': bzone
        }
        item = 'getmarketload'

        df = self._base_request(item, data)
        return df
    
    def get_actual_generation(self, start: pd.Timestamp, end: pd.Timestamp, gen_type: str) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        gen_type : str
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
            'type': gen_type
        }
        item = 'getactualgeneration'

        df = self._base_request(item, data)
        return df
    
    def get_installed_capacity(self, year: int, gen_type: str) -> pd.DataFrame:
        """
        Parameters
        ----------
        gen_type : str
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'year': year,
            'type': gen_type
        }
        item = 'getinstalledcapacity'

        df = self._base_request(item, data)
        return df
        
    def get_scheduled_foreign_exchange(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
        }
        item = 'getscheduledforeignexchange'

        df = self._base_request(item, data)
        return df
    
    def get_scheduled_internal_exchange(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
        }
        item = 'getscheduledinternalexchange'

        df = self._base_request(item, data)
        return df
        
    def get_physical_foreign_flow(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
        }
        item = 'getphysicalforeignflow'

        df = self._base_request(item, data)
        return df
    
    def get_physical_internal_flow(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        
        Returns
        -------
        pd.DataFrame
        """
        
        data = {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
        }
        item = 'getphysicalinternalflow'

        df = self._base_request(item, data)
        return df
    
def adjust_tz(dt, tz):
    delta = dt.minute % 15
    if delta == 0:
        return dt.tz_localize(tz, ambiguous=True)
    else:
        return (dt - datetime.timedelta(minutes=delta+15*(4-delta))).tz_localize(tz, ambiguous=False)
        
        