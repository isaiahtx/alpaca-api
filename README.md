# Alpaca API

A small python package for downloading data from the [Alpaca
API](https://docs.alpaca.markets/reference) without having to deal with API
limits.

## Usage

In order to install the package, use the following command in your python script:
```
pip install git+https://github.com/isaiahtx/alpaca-api.git
```
In order to use this package, you will need to create a file named `.env`
somewhere in your project directory containing at the following lines:
```
APCA_API_KEY_ID=<your_api_key>
APCA_API_SECRET_KEY=<your_secret_key>
```
Then you can import the `AlpacaRequester` class as follows:
```python
from alpaca_api import AlpacaRequester
```

## Example

There is an example notebook located at
[`examples/eg.ipynb`](examples/eg.ipynb) for example usage. In order to run it,
download the [uv](https://docs.astral.sh/uv/) environment manager, and run `uv
sync` followed by `uv pip install -e ".[dev]"`.
