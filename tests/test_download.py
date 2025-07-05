from alpaca_api import AlpacaRequester
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import pytest
import os

def test_get_account_info_from_env():
    X = AlpacaRequester()
    load_dotenv()
    assert(X.api_key == os.getenv('APCA_API_KEY_ID'))
    assert(X.api_secret == os.getenv('APCA_API_SECRET_KEY'))

def test_get_account_info_from_args():
    api_key = os.getenv('APCA_API_KEY_ID')
    api_secret = os.getenv('APCA_API_SECRET_KEY')

    X = AlpacaRequester(api_key=api_key,api_secret=api_secret)

    assert(X.api_key == api_key)
    assert(X.api_secret == api_secret)

def test_get_data_no_auth():
    X = AlpacaRequester(api_key='a',api_secret='b')
    with pytest.raises(ValueError) as e:
        X.get_bars("https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL&timeframe=5T&start=2025-01-01&limit=10000&adjustment=raw&feed=sip&sort=asc")
    
    assert "403" in str(e.value)

def test_get_account_info_no_env():

    path = find_dotenv()

    if path != '':
        old_path = Path(path)
        tmp_path = old_path.with_name('.env.tmp')
        old_path.rename(tmp_path)
        assert(not load_dotenv())

    with pytest.raises(ValueError):
        Y = AlpacaRequester()

    tmp_path.rename(old_path)

def test_get_bars():
    """
    Downloads data from URL until no pages left.
    """
    base_url = "https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL&timeframe=5T&start=2025-01-01&limit=10000&adjustment=raw&feed=sip&sort=asc"

    X = AlpacaRequester()
