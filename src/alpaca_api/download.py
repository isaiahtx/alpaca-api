from dotenv import load_dotenv
import os
from pathlib import Path
import pandas as pd
import requests
import logging
from rich.logging import RichHandler
from datetime import datetime
from tqdm import tqdm
import time
from typing import Set, Dict, Callable, TypeVar, Optional, Any
from collections.abc import Iterable

T = TypeVar('T')

logger = logging.getLogger("alpaca_api")
logger.addHandler(logging.NullHandler())

NEWS_BASE_URL = "https://data.alpaca.markets/v1beta1/news"
BARS_BASE_URL = "https://data.alpaca.markets/v2/stocks/bars"
CALENDAR_BASE_URL = "https://paper-api.alpaca.markets/v2/calendar"

class AlpacaRequester:
    api_key: str
    api_secret: str
    headers: Dict[str,str]
    pbars: Set[tqdm]

    def __init__(self,
            api_key:Optional[str]=None,
            api_secret:Optional[str]=None,
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
        """
        logger = logging.getLogger("alpaca_api")
        logger.setLevel(level)
        logger.propagate = False  # don't spam the root logger

        # Clear existing handlers so we don't duplicate logs on repeated calls
        for h in list(logger.handlers):
            logger.removeHandler(h)

        if logfile_path is None:
            logfile_path = f"logs/request_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log"

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
    def make_url(base:str, **kwargs:Any) -> str:
        if not kwargs:
            return base
        
        params = []
        for k, v in kwargs.items():
            if isinstance(v, Iterable) and not isinstance(v, (str, bytes, bytearray)):
                joined = "%2C".join(str(v1) for v1 in v)
                params.append(f"{k}={joined}")
            else:
                params.append(f"{k}={v}")

        final = f"{base}?{'&'.join(params)}"
        return final

    def paginate(
            self,
            base: str,
            api_options: Dict[str,Any],
            data_fmt: Callable[[Dict],Dict[str,pd.DataFrame]],
            write_path: str,
            logfile_path: str,
            log_fmt: Optional[Callable[[Dict[str,pd.DataFrame]], str]] = None,
            verbose: bool = False,
        ) -> None:

        logger = AlpacaRequester.configure_logging(
            level=logging.INFO,
            logfile_path=logfile_path,
            to_console=verbose,
        )
        logger.info(f"Logging to {logfile_path}")

        pbar = tqdm(total=0, desc="Fetching pages")
        self.pbars.add(pbar)

        next_page_token = api_options.pop("page_token", None)
        base_url = self.make_url(base,**api_options)
        url = base_url if next_page_token is None else f"{base_url}&page_token={next_page_token}"
        num_pages = 0

        while True:
            logger.info("--------------------------------")
            logger.info(f"Requesting data from {url}")
            response = requests.get(url, headers=self.headers)

            if response.status_code == 429:
                logger.error(f"Error Status Code {response.status_code}: Rate limit exceeded. Waiting for 5 seconds and retrying...")
                time.sleep(5)
                continue
            elif response.status_code == 403:
                raise ValueError(
                    f"Error Status Code {response.status_code}: Authentication headers are missing or invalid. Make sure you authenticate your request with valid API credentials."
                )
            elif response.status_code != 200:
                raise ValueError(f"Error Status Code {response.status_code}: {response.text}")

            if num_pages == 0:
                logger.info(f"Initial request successful, status code: {response.status_code}")
            num_pages += 1
            pbar.update(1)

            rate_limit = int(response.headers["X-RateLimit-Limit"])
            rate_limit_remaining = int(response.headers["X-RateLimit-Remaining"])
            logger.info(f"Rate limit remaining: {rate_limit_remaining}/{rate_limit}")

            body = response.json()
            next_page_token = body["next_page_token"]

            tables = data_fmt(body)
            for name,df in tables.items():
                path = Path(write_path.format(name))
                path.parent.mkdir(parents=True, exist_ok=True)
                has_rows = path.is_file() and path.stat().st_size > 0
                df['url'] = url
                df['next_page_token'] = next_page_token
                df.to_csv(
                    path,
                    mode = 'a' if has_rows else 'w',
                    header = not has_rows,
                    index = False
                )
                has_rows = True

            if log_fmt is not None:
                logger.info(log_fmt(tables))

            if next_page_token is None:
                logger.info("Finished fetching pages")
                break

            if rate_limit_remaining == 0:
                rate_limit_reset = int(response.headers["X-RateLimit-Reset"])
                to_wait = (rate_limit_reset - time.time()) * 0.5
                logger.info(f"Rate limit reached, waiting for {to_wait} seconds")
                time.sleep(to_wait)

            url = f"{base_url}&page_token={next_page_token}"

        pbar.close()
        self.pbars.discard(pbar)

    @close_pbar_on_exception
    def get_bars(self,verbose=False,write_path="bars/{}.csv",**kwargs) -> None:
        """
        https://docs.alpaca.markets/reference/stockbars
        """
        kwargs['limit'] = 10_000  # maximum limit per request is 10,000
        self.paginate(
            base=BARS_BASE_URL,
            api_options=kwargs,
            data_fmt=lambda body: {k:pd.DataFrame(v).assign(t=lambda df: pd.to_datetime(df['t'])) for k,v in body['bars'].items()},
            write_path=write_path,
            logfile_path=f"logs/bars_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log",
            log_fmt=lambda tables: "Wrote:\n\t- " + "\n\t- ".join(
                f"{k}: {v.iloc[0]['t']} to {v.iloc[-1]['t']} ({len(v)} bars)" for k, v in tables.items()
            ),
            verbose=verbose,
        )

        print("Wrote bars to files")

    @close_pbar_on_exception
    def get_news(self,verbose:bool=False,write_path:str="news/news.csv",**kwargs) -> None:
        """
        https://docs.alpaca.markets/reference/news-3
        """
        kwargs['limit'] = 50  # maximum limit per request is 50
        self.paginate(
            base=NEWS_BASE_URL,
            api_options=kwargs,
            data_fmt=lambda x: {'':pd.DataFrame(x['news'])},
            write_path=write_path,
            logfile_path=f"logs/news_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log",
            log_fmt=lambda tables: f"{len(tables[''])} articles written spanning {tables[''].iloc[-1]['created_at']} to {tables[''].iloc[0]['created_at']}",
            verbose=verbose,
        )

        print("Wrote news to files")
    
    def market_calendar(self,**kwargs) -> pd.DataFrame:
        """
        https://docs.alpaca.markets/reference/getcalendar-1
        """
        url = self.make_url(CALENDAR_BASE_URL, **kwargs)
        response = requests.get(url, headers=self.headers)
        data = response.json()
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df