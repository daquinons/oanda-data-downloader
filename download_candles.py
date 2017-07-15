"""download the candles from fxpractice.oanda.com with no time limits, by
chaining multiple requests."""

import datetime
import time
import requests
import pandas
import json
import sys
import click

# http://developer.oanda.com/rest-live/rates/#retrieveInstrumentHistory
granularities = [
    "S5",  # 5 seconds
    "S10",  # 10 seconds
    "S15",  # 15 seconds
    "S30",  # 30 seconds
    "M1",  # 1 minute
    "M2",  # 2 minutes
    "M3",  # 3 minutes
    "M4",  # 4 minutes
    "M5",  # 5 minutes
    "M10",  # 10 minutes
    "M15",  # 15 minutes
    "M30",  # 30 minutes
    "H1",  # 1 hour
    "H2",  # 2 hours
    "H3",  # 3 hours
    "H4",  # 4 hours
    "H6",  # 6 hours
    "H8",  # 8 hours
    "H12",  # 12 hours
    "D",  # 1 Day
    "W",  # 1 Week
]

duration = {
    "S5": datetime.timedelta(seconds=5),
    "S10": datetime.timedelta(seconds=10),
    "S15": datetime.timedelta(seconds=15),
    "S30": datetime.timedelta(seconds=30),
    "M1": datetime.timedelta(minutes=1),
    "M2": datetime.timedelta(minutes=2),
    "M3": datetime.timedelta(minutes=3),
    "M4": datetime.timedelta(minutes=4),
    "M5": datetime.timedelta(minutes=5),
    "M10": datetime.timedelta(minutes=10),
    "M15": datetime.timedelta(minutes=15),
    "M30": datetime.timedelta(minutes=30),
    "H1": datetime.timedelta(hours=1),
    "H2": datetime.timedelta(hours=2),
    "H3": datetime.timedelta(hours=3),
    "H4": datetime.timedelta(hours=4),
    "H6": datetime.timedelta(hours=6),
    "H8": datetime.timedelta(hours=8),
    "H12": datetime.timedelta(hours=12),
    "D": datetime.timedelta(days=1),
    "W": datetime.timedelta(weeks=1),
}

def get_data_candles(oanda_token, instrument, granularity, begin, end):
    url = "https://api-fxpractice.oanda.com/v1/candles"
    start = datetime.datetime(begin, 1, 1)
    end = datetime.datetime(end - 1, 12, 31, 23, 59, 59)
    headers = {"Authorization": "Bearer " + oanda_token}
    params = {
        "instrument": instrument,
        "granularity": granularity,
    }
    current = start
    delta = 4500 * duration[granularity]
    data_columns = ["open_ask", "close_ask", "high_ask", "low_ask", "open_bid", "close_bid", "high_bid", "low_bid", "volume"]
    data = pandas.DataFrame(columns=data_columns)
    while current < end:
        params["start"] = current.isoformat()
        params["end"] = (current + delta).isoformat()
        print("Requesting data from", params["start"], file=sys.stderr)
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code is not 200:
                raise requests.exceptions.HTTPError(response.status_code)
            candles = response.json()["candles"]
            for candle in candles:
                try:
                    date_time = candle["time"]
                    open_ask = candle["openAsk"]
                    close_ask = candle["closeAsk"]
                    high_ask = candle["highAsk"]
                    low_ask = candle["lowAsk"]
                    open_bid = candle["openBid"]
                    close_bid = candle["closeBid"]
                    high_bid = candle["highBid"]
                    low_bid = candle["lowBid"]
                    volume = candle["volume"]
                    data_line = pandas.DataFrame({
                                                "open_ask": [open_ask],
                                                "close_ask": [close_ask],
                                                "high_ask": [high_ask],
                                                "low_ask": [low_ask],
                                                "open_bid": [open_bid],
                                                "close_bid": [close_bid],
                                                "high_bid": [high_bid],
                                                "low_bid": [low_bid],
                                                "volume": [volume]
                                                }, index=[pandas.to_datetime([date_time])])
                    data = data.append(data_line)

                except KeyError:
                    print(response.json(), file=sys.stderr)
                    break
        except requests.exceptions.HTTPError as e:
            print("Status code:", e)


        current += delta
        time.sleep(0.5)

    data.index.name = "datetime"

    return data

@click.command()
@click.option(
    "--oanda-token",
    default="ADD YOUR TOKEN HERE",
    help="access token for the oanda fxpractice api"
)
@click.option("--instrument", default="EUR_USD",
              help="request candles for this instrument")
@click.option("--granularity", type=click.Choice(granularities), default="H1")
@click.option("--begin", type=int, default=2014)
@click.option("--end", type=int, default=2015)
@click.option("--path", type=click.Path(), default=None, help="Optional: set the place where the csv will be stored and its name. Ex: './EURUSD_1H.csv'")
def download_candles(oanda_token, instrument, granularity, begin, end, path=None):
    """

    :oanda_token: a valid fxpractice token
    :instrument: an oanda instrument e.g. EUR_USD
    :granularity: http://developer.oanda.com/rest-live/rates/#retrieveInstrumentHistory
    :begin: a year
    :end: a year
    :path: the complete path for the csv file to be saved

    """
    data = get_data_candles(oanda_token, instrument, granularity, begin, end)

    if path is None:
        path = "./" + instrument + "_" + granularity + ".csv"
    data.to_csv(path)
    print("Saved to", path)
