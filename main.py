import Query_Data
import Chart
from datetime import datetime
import pandas as pd
import os


def price_change_daily():
    timeframe = 'daily'
    rank_size = 20

    file = 'data/price_change_daily.csv'
    if not os.path.isfile(file):
        result = Query_Data.price_change(timeframe=timeframe, rank_size=rank_size)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.price_change(df=result, timeframe=timeframe)


def price_change_weekly():
    timeframe = 'weekly'
    rank_size = 20

    file = 'data/price_change_weekly.csv'
    if not os.path.isfile(file):
        result = Query_Data.price_change(timeframe=timeframe, rank_size=rank_size)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.price_change(df=result, timeframe=timeframe)


def asset_volume_rank():
    exchange_list = ['binance', 'gdax', 'kraken', 'okex', 'bitfinex', 'bitstamp', 'mexc', 'gemini', 'bybit', 'huobi']
    asset_list = ['btc', 'eth', 'bnb', 'xrp', 'ada', 'doge', 'matic', 'sol', 'dot', 'shib', 'ltc', 'trx', 'avax', 'uni']
    start = datetime(2023, 1, 1).timestamp()
    end = datetime(2023, 1, 8).timestamp()
    timeframe = 7  # days

    file = 'data/asset_volume_rank.csv'
    if not os.path.isfile(file):
        result = Query_Data.asset_volume_rank(exchange_list=exchange_list, asset_list=asset_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.asset_volume_rank(df=result, timeframe=timeframe)


def exchange_volume_rank():
    exchange_list = ['binance', 'gdax', 'kraken', 'okex', 'bitfinex', 'bitstamp', 'mexc', 'gemini', 'bybit', 'huobi']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()
    timeframe = '1 month'

    file = 'data/exchange_volume_rank.csv'
    if not os.path.isfile(file):
        result = Query_Data.exchange_volume_rank(exchange_list=exchange_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.exchange_volume_rank(df=result, timeframe=timeframe)


def exchange_volume_history_total():
    exchange_list = ['binance', 'gdax', 'kraken', 'okex', 'bitfinex', 'bitstamp', 'mexc', 'gemini', 'bybit', 'huobi']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()

    file = 'data/vol_history_agg.csv'
    if not os.path.isfile(file):
        result = Query_Data.exchange_volume_history_total(exchange_list=exchange_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.exchange_volume_history_total(df=result)


def asset_volume_history_agg():
    exchange_list = ['binance', 'gdax', 'kraken', 'okex', 'bitfinex', 'bitstamp', 'mexc', 'gemini', 'bybit', 'huobi']
    asset_list = ['btc', 'eth', 'bnb', 'xrp', 'ada', 'doge', 'matic', 'sol', 'dot', 'shib', 'ltc', 'trx', 'avax', 'uni']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()

    file = 'data/asset_volume_history_agg.csv'
    if not os.path.isfile(file):
        result = Query_Data.asset_volume_history_agg(exchange_list=exchange_list, asset_list=asset_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.asset_volume_proportion_history(df=result)


def funding_rate_history():
    exchange = 'binance'
    pair_list = ['BTCUSD_PERP', 'ETHUSD_PERP', 'BNBUSD_PERP']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()

    file = 'data/funding_rate_history_agg.csv'
    if not os.path.isfile(file):
        result = Query_Data.funding_rate_history_agg(exchange=exchange, pair_list=pair_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.funding_rate_history(df=result, exchange=exchange)


def open_interest_history():
    exchange = 'binance'
    pair_list = ['BTCUSDT', 'ETHUSDT']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()

    file = f'data/{exchange}_open_interest_history_agg.csv'
    if not os.path.isfile(file):
        result = Query_Data.open_interest_history_agg(exchange=exchange, pair_list=pair_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.open_interest_history(df=result, exchange=exchange, pair_list=pair_list)


def liquidation_history():
    exchange = 'binance'
    pair_list = ['BTCUSDT', 'ETHUSDT']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()

    file = f'data/{exchange}_liquidation_history_agg.csv'
    if not os.path.isfile(file):
        result = Query_Data.liquidation_history(exchange=exchange, pair_list=pair_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.liquidation_history(df=result, exchange=exchange, pair_list=pair_list)


def long_short_ratio_history():
    exchange = 'binance'
    pair_list = ['BTCUSDT', 'ETHUSDT']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2022, 12, 2).timestamp()

    file = f'data/{exchange}_long_short_ratio_history_agg.csv'
    if not os.path.isfile(file):
        result = Query_Data.long_short_ratio_history(exchange=exchange, pair_list=pair_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.long_short_ratio_history(df=result, exchange=exchange)


def nvt_ratio_history():
    asset_list = ['eth', 'btc']
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()

    file = 'data/nvt_ratio_agg.csv'
    if not os.path.isfile(file):
        result = Query_Data.nvt_ratio_history(asset_list=asset_list, start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.nvt_ratio_history(df=result)


def ethereum_gas_fee_in_usd():
    start = datetime(2022, 12, 1).timestamp()
    end = datetime(2023, 1, 1).timestamp()

    file = 'data/ethereum_gas_fee_in_usd.csv'
    if not os.path.isfile(file):
        result = Query_Data.ethereum_gas_fee_in_usd(start=start, end=end)
        result.to_csv(file)
    else:
        result = pd.read_csv(file)
    Chart.ethereum_gas_fee_in_usd(df=result)


if __name__ == '__main__':

    os.makedirs('chart', exist_ok=True)
    os.makedirs('data', exist_ok=True)

    price_change_daily()
    price_change_weekly()
    asset_volume_rank()
    exchange_volume_rank()
    exchange_volume_history_total()
    asset_volume_history_agg()
    funding_rate_history()
    open_interest_history()
    liquidation_history()
    long_short_ratio_history()
    nvt_ratio_history()
    ethereum_gas_fee_in_usd()