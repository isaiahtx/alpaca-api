from dotenv import load_dotenv
from collections import defaultdict
import os
from pathlib import Path
import pandas as pd
import requests
import logging
from rich.logging import RichHandler
from datetime import datetime, timezone
from tqdm import tqdm
import time
from typing import Set, Dict, List, Callable, TypeVar, Optional, Union, Mapping, Tuple

T = TypeVar('T')

logger = logging.getLogger("alpaca_api")
logger.addHandler(logging.NullHandler())

class AlpacaRequester:
    api_key: str
    api_secret: str
    headers: Dict[str,str]
    pbars: Set[tqdm]
    bars_path: str

    def __init__(self,
            api_key:Optional[str]=None,
            api_secret:Optional[str]=None,
            bars_path="bars/{}.csv",
        ) -> None:
        if api_key is None or api_secret is None:
            if load_dotenv():
                api_key = os.getenv("APCA_API_KEY_ID")
                api_secret = os.getenv("APCA_API_SECRET_KEY")
                if api_key is None or api_secret is None:
                    raise ValueError("Please set the APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables in the .env file or pass them as arguments")
            else:
                raise ValueError("No .env file found, please set the APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables in the .env file or pass them as arguments")

        self.api_key = api_key
        self.api_secret = api_secret

        self.headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret
        }

        self.bars_path = bars_path
        self.pbars = set()

    def close_pbars(self) -> None:
        for pbar in self.pbars:
            pbar.close()
        self.pbars.clear()

    @staticmethod
    def close_pbar_on_exception(func: Callable[...,T]) -> Callable[...,T]:
        def wrapper(self, *args, **kwargs) -> T:
            try:
                output = func(self, *args, **kwargs)
                logging.getLogger("alpaca_api").debug("Closing progress bars")
                self.close_pbars()
                return output
            except BaseException:
                logging.getLogger("alpaca_api").debug("Closing progress bars")
                self.close_pbars()
                raise
        return wrapper
    
    @staticmethod
    def configure_logging(
        level:int = logging.INFO,
        logfile_path: Optional[str] = None,
        to_console: bool = False
    ) -> logging.Logger:
        """
        Configure and return the library-scoped logger ("alpaca_api").

        Calling this multiple times is safe; existing handlers are cleared to avoid duplicate log records.
        """
        logger = logging.getLogger("alpaca_api")
        logger.setLevel(level)
        logger.propagate = False  # don't spam the root logger

        # Clear existing handlers so we don't duplicate logs on repeated calls
        for h in list(logger.handlers):
            logger.removeHandler(h)

        if logfile_path is None:
            logfile_path = f"logs/request_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log"

        # Ensure directory exists
        os.makedirs(os.path.dirname(logfile_path), exist_ok=True)

        file_handler = logging.FileHandler(logfile_path, mode="a")
        formatter = logging.Formatter(
            "%(asctime)s [%(name)s] [%(levelname)s] %(message)s", datefmt="[%X]"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if to_console:
            logger.addHandler(RichHandler())

        return logger
    
    @staticmethod
    def make_bars_url(
            symbols: Union[str,List[str]],
            timeframe: str,
            **kwargs: Union[str,int]
        ) -> str:
        """
        https://docs.alpaca.markets/reference/stockbars

        E.g.: https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL%2CTSLA&timeframe=1T&start=2024-01-01&end=2025-01-01&limit=10000&adjustment=all&asof=2025-01-01&feed=sip&currency=USD&page_token=PAGE_TOKEN&sort=asc
        """
        if isinstance(symbols,str):
            symbols = [symbols]

        if not ("limit" in kwargs):
            kwargs["limit"] = 10000
        if "sort" in kwargs:
            kwargs.pop("sort")
            logging.getLogger("alpaca_api").warning("Sorting parameter is ignored")

        return f"""https://data.alpaca.markets/v2/stocks/bars?symbols={'%2C'.join(symbols)}&timeframe={timeframe}{''.join(f"&{k}={v}" for k,v in kwargs.items())}"""

    def write_bars(self,
            bars: Dict[str,List[Dict[str,Union[str,int,float]]]],
            download_url: str,
            next_page_token: Optional[str] = None
        ) -> Dict[str,Dict[str,Union[datetime,int]]]:
        output = {k:{} for k in bars.keys()}
        for ticker,entries in bars.items():
            path = Path(self.bars_path.format(ticker))
            has_rows = path.is_file() and path.stat().st_size > 0
            entries_df = pd.DataFrame(entries)
            entries_df['t'] = pd.to_datetime(entries_df['t'])
            entries_df['url'] = download_url
            entries_df['next_page_token'] = next_page_token
            entries_df.to_csv(
                path,
                mode = 'a' if has_rows else 'w',
                header = not has_rows,
                index = False
            )
            output[ticker]['start'] = entries_df.iloc[0]['t']
            output[ticker]['end'] = entries_df.iloc[-1]['t']
            output[ticker]['count'] = len(entries_df)
        
        return output

    @close_pbar_on_exception
    def get_bars(self,
            symbols: Union[str,List[str]],
            timeframe: str,
            api_options: Dict[str,str] = {},
            verbose: bool = False,      # There is not much reason to set this to True...
            logfile_path: Optional[str] = None,
        ) -> None:
        """
        https://docs.alpaca.markets/reference/stockbars
        """
        # Set up logging
        if logfile_path is None:
            logfile_path = f"logs/bars_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log"

        logger = AlpacaRequester.configure_logging(
            level=logging.INFO,
            logfile_path=logfile_path,
            to_console=verbose
        )
        logger.info(f"Logging to {logfile_path}")

        start_page_token = api_options.pop("page_token",None)
        base_url = self.make_bars_url(symbols,timeframe,**api_options) 
        url = base_url if start_page_token is None else f"{base_url}&page_token={start_page_token}"
        
        # Set up progress bar
        pbar = tqdm(total=0,desc="Fetching pages")
        self.pbars.add(pbar)
        pbar.update(0)

        # Make initial request
        logger.info("--------------------------------")
        logger.info(f"MAKING INITIAL REQUEST")
        logger.info("--------------------------------")
        logger.info(f"Requesting data from {url}")
        response = requests.get(url, headers=self.headers)
        
        next_page_token = start_page_token
        if response.status_code == 200:  # 200 indicates success
            logger.info(f"Initial request successful, status code: {response.status_code}")
            pbar.update(1)
            rate_limit = int(response.headers['X-RateLimit-Limit'])
            rate_limit_remaining = int(response.headers['X-RateLimit-Remaining'])
            rate_limit_reset = int(response.headers['X-RateLimit-Reset'])
            logger.info(f"Rate limit: {rate_limit}")
            logger.info(f"Rate limit remaining: {rate_limit_remaining}")
            logger.info(f"Rate limit reset: {datetime.fromtimestamp(rate_limit_reset)}")

            body = response.json()
            next_page_token = body['next_page_token']
            written = self.write_bars(body['bars'],url,next_page_token)
            logger.info(f"Wrote:\n\t- {'\n\t- '.join(f"{k}: {v['start']} to {v['end']} ({v['count']} bars)" for k,v in written.items())}")

            logger.info("--------------------------------")
            logger.info(f"MAKING FURTHER REQUESTS")
            logger.info("--------------------------------")

            num_pages = 1
            while True:
                if next_page_token is None: # Check if we're at last page
                    logger.info(f"Finished fetching pages")
                    break
                elif rate_limit_remaining == 0: # Check if we've hit the rate limit
                    # API works by simply giving you a request token every 60/rate_limit seconds, up to rate_limit tokens
                    # Wait until API limit is half full
                    to_wait = (rate_limit_reset - time.time()) * 0.5 
                    logger.info(f"Rate limit exceeded, waiting for {to_wait} seconds")
                    time.sleep(to_wait)
                    logger.info("Continuing")

                url = f"{base_url}&page_token={next_page_token}"
                response = requests.get(url, headers=self.headers)
                #logger.info(response.json())

                if response.status_code != 200: # Check if request was successful
                    logger.error(f"Error Status Code {response.status_code}: {response.text}")
                    raise ValueError(f"Error Status Code {response.status_code}: {response.text}")

                body = response.json()
                next_page_token = body['next_page_token']
                written = self.write_bars(body['bars'],url,next_page_token)

                num_pages += 1
                pbar.update(1)

                rate_limit_remaining = int(response.headers['X-RateLimit-Remaining'])
                rate_limit_reset = int(response.headers['X-RateLimit-Reset'])

                logger.info(f"Fetched page {next_page_token}, remaining rate limit: {rate_limit_remaining}/{rate_limit}, {num_pages} pages\n\t- {'\n\t- '.join(f"{k}: {v['start']} to {v['end']} ({v['count']} bars)" for k,v in written.items())}")
        
            print("Wrote bars to files")

        elif response.status_code == 429:
            logger.error(f"Error Status Code {response.status_code}: Rate limit exceeded. Waiting for 5 seconds and retrying...")
            rate_limit = response.headers['X-RateLimit-Limit']
            rate_limit_remaining = response.headers['X-RateLimit-Remaining']
            rate_limit_reset = response.headers['X-RateLimit-Reset']
            logger.info(f"Rate limit: {rate_limit}")
            logger.info(f"Rate limit remaining: {rate_limit_remaining}")
            logger.info(f"Rate limit reset: {datetime.fromtimestamp(int(rate_limit_reset))}")
            time.sleep(5)
            return self.get_bars(symbols,timeframe,api_options,verbose,logfile_path)

        elif response.status_code == 403:
            error_msg = f"Error Status Code {response.status_code}: Authentication headers are missing or invalid. Make sure you authenticate your request with valid API credentials."
            logger.error(error_msg)
            raise ValueError(error_msg)

        else:
            error_msg = f"Error Status Code {response.status_code}: {response.text}"
            logger.error(error_msg)
            raise ValueError(error_msg)