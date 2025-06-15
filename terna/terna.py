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
import sys
from typing import Optional, Dict
from urllib.parse import urlencode

__title__ = "terna-py"
__version__ = "0.5.2"
__author__ = "fgenoese"
__license__ = "MIT"

URL = 'https://api.terna.it/transparency/oauth/accessToken'
BASE_URL = 'https://api.terna.it/'
RATE_LIMIT = 1.1  # seconds between requests to respect the rate limit

class TernaPandasClient:
    def __init__(
            self, api_key: str, api_secret: str, session: Optional[requests.Session] = None,
            proxies: Optional[Dict] = None, timeout: Optional[int] = None,
            log_level: Optional[int] = logging.ERROR):
        """
        Parameters
        ----------
        api_client : str
        api_secret : str
        session : requests.Session
        proxies : dict
            requests proxies
        timeout : int
        log_level : int, optional
            Logging level (default: logging.ERROR)
        """
        
        # Set up logger
        log = logging.getLogger(__name__)
        log.setLevel(log_level)
        log.propagate = False  # prevent double logging
        if not log.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            log.addHandler(handler)
        self.logger = log
        self.logger.debug("Client initialized with log level %s", logging.getLevelName(log_level))
        
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
        self.token = None
        self.token_expiration = datetime.datetime.now()
        self.time_of_last_request = time.monotonic() - 1

    def _request_token(self, data: Dict = {}) -> str:
        """
        Parameters
        ----------
        data : dict
        
        Returns
        -------
        access_token : str
        """
        
        if self.token_expiration + datetime.timedelta(seconds=5) < datetime.datetime.now() and self.token:
            return self.token

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
            time_elapsed = time.monotonic() - self.time_of_last_request
            if time_elapsed < RATE_LIMIT:
                self.logger.debug("[token] Waiting for %.2f seconds to respect rate limit", RATE_LIMIT - time_elapsed)
                time.sleep(RATE_LIMIT - time_elapsed)
            response = self.session.post(URL, headers=headers, data=data)
            self.time_of_last_request = time.monotonic()
            response.raise_for_status()
            self.logger.debug(response.text)

        except requests.HTTPError as exc:
            code = exc.response.status_code
            if code in [429, 500, 502, 503, 504]:
                self.logger.error(code)
            raise
        
        else:
            if response.status_code == 200:
                token = response.json().get('access_token')
                expires_in = response.json().get('expires_in')
                self.token_expiration = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
                self.token = token
                return token
            else:
                self.logger.error(f"Request failed with status code {response.status_code}")
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
        params = urlencode(data, doseq=True)
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        _url = f"{BASE_URL}{item}"
        self.logger.debug("API endpoint: " + _url)
        
        try:
            time_elapsed = time.monotonic() - self.time_of_last_request
            if time_elapsed < RATE_LIMIT:
                self.logger.debug("[base request] Waiting for %.2f seconds to respect rate limit", RATE_LIMIT - time_elapsed)
                time.sleep(RATE_LIMIT - time_elapsed)
            response = self.session.get(_url, headers=headers, params=params)
            self.time_of_last_request = time.monotonic()
            response.raise_for_status()
            self.logger.debug(response.text[:500])

        except requests.HTTPError as exc:
            code = exc.response.status_code
            if code in [429, 500, 502, 503, 504]:
                self.logger.error(f"Request failed with status code {code}")
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
                        df['Date'] = df['Date'].map(lambda x: TernaPandasClient._adjust_tz(x, tz="Europe/Rome"))
                        df.sort_values(by='Date', inplace=True)
                        df.index = df['Date']
                        df.index.name = None
                        df.drop(columns=['Date'], inplace=True)
                    elif 'Year' in df.columns:
                        df.index = df['Year']
                        df.drop(columns=['Year'], inplace=True)
                    for col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except (ValueError, TypeError):
                            pass
                    return df
                else:
                    return None
            else:
                self.logger.error(f"Request failed with status code {response.status_code}")
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
        
        data = TernaPandasClient._build_date_range_payload(start, end)
        data.update({'biddingZone': bzone})

        item = 'load/v2.0/total-load'

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
        
        data = TernaPandasClient._build_date_range_payload(start, end)
        data.update({'biddingZone': bzone})

        item = 'load/v2.0/market-load'

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

        item = 'generation/v2.0/actual-generation'

        data = TernaPandasClient._build_date_range_payload(start, end)
        data.update({'type': gen_type})

        df = self._base_request(item, data)
        return df
    
    def get_renewable_generation(self, start: pd.Timestamp, end: pd.Timestamp, res_gen_type: str) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        res_gen_type : str
        
        Returns
        -------
        pd.DataFrame
        """

        item = 'generation/v2.0/renewable-generation'

        data = TernaPandasClient._build_date_range_payload(start, end)
        data.update({'type': res_gen_type})

        df = self._base_request(item, data)
        return df
    
    def get_energy_balance(self, start: pd.Timestamp, end: pd.Timestamp, type: str) -> pd.DataFrame:
        """
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        type : str
        
        Returns
        -------
        pd.DataFrame
        """

        item = 'generation/v2.0/energy-balance'
        
        data = TernaPandasClient._build_date_range_payload(start, end)
        data.update({'type': type})

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
        
        item = 'generation/v2.0/installed-capacity'

        data = {
            'year': year,
            'type': gen_type
        }

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

        item = 'transmission/v2.0/scheduled-foreign-exchange'

        data = TernaPandasClient._build_date_range_payload(start, end)

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
        
        item = 'transmission/v2.0/scheduled-internal-exchange'

        data = TernaPandasClient._build_date_range_payload(start, end)

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
        
        item = 'transmission/v2.0/physical-foreign-flow'

        data = TernaPandasClient._build_date_range_payload(start, end)

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
        
        item = 'transmission/v2.0/physical-internal-flow'

        data = TernaPandasClient._build_date_range_payload(start, end)

        df = self._base_request(item, data)
        return df

    @staticmethod
    def _build_date_range_payload(start: pd.Timestamp, end: pd.Timestamp) -> dict:
        """
        Internal helper to build a date range dictionary formatted as 'DD/MM/YYYY'.

        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        dict
            A dictionary with formatted 'dateFrom' and 'dateTo' strings.
        """
        return {
            'dateFrom': start.strftime('%d/%m/%Y'),
            'dateTo': end.strftime('%d/%m/%Y'),
        }

    @staticmethod
    def _adjust_tz(dt, tz):
        delta = dt.minute % 15
        if delta == 0:
            return dt.tz_localize(tz, ambiguous=True)
        else:
            return (dt - datetime.timedelta(minutes=delta+15*(4-delta))).tz_localize(tz, ambiguous=False)

    def __repr__(self):
        return f"<TernaPandasClient(api_key={self.api_key[:4]}***, api_secret={self.api_secret[:4]}***)>"
    
     