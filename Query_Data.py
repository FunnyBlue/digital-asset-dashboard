import Amber_api
from datetime import datetime
import pandas as pd

api_key = 'UAK3484b3d443f4b50d3a915a616d24c66c'
report_data = Amber_api.amber_api(api_key)


def asset_volume_history_agg(exchange_list, asset_list, start, end):
    df_asset_volume_history_agg = pd.DataFrame()
    for i in exchange_list:
        df_asset_volume_history_in_exchange = pd.DataFrame()

        for j in asset_list:
            result = report_data.asset_volume_history(exchange=i, asset=j, start=start, end=end)
            print(result)
            if len(result) > 0:
                df_asset_volume_history_in_exchange = pd.concat([df_asset_volume_history_in_exchange, result], ignore_index=True)

        print(df_asset_volume_history_in_exchange)
        df_asset_volume_history_agg = pd.concat([df_asset_volume_history_agg, df_asset_volume_history_in_exchange], ignore_index=True)

    return df_asset_volume_history_agg


def price_change(timeframe, rank_size):
    """
    1. top 300 market cap
    2. timeframe: daily, weekly
    """
    result = report_data.asset_ranking_by_marketcap_latest(size=300)
    if timeframe == 'daily':
        result = result.sort_values('changeInPriceDaily', ascending=False).iloc[:rank_size]
    elif timeframe == 'weekly':
        result = result.sort_values('changeInPriceWeekly', ascending=False).iloc[:rank_size]

    print(result)

    return result


def asset_volume_rank(exchange_list, asset_list, start, end):
    """
    1. if quote_volume true >> asset not found in the specific exchange
    2. bnb , xrp, trx ... not listed in some exchange
    3. asset ranking history doesn't have tradeVolume
    """
    df_asset_volume_history_agg = asset_volume_history_agg(exchange_list=exchange_list, asset_list=asset_list, start=start, end=end)
    df_asset_volume_rank = df_asset_volume_history_agg.groupby('asset').agg(volumeUSD=('volumeUSD', 'sum')).sort_values('volumeUSD', ascending=False).reset_index()
    print(df_asset_volume_rank)
    return df_asset_volume_rank


def exchange_volume_rank(exchange_list, start, end):
    df_volume_history_agg = report_data.exchange_volume_history(start=start, end=end)
    df_volume_history_agg = df_volume_history_agg.loc[df_volume_history_agg['exchange'].isin(exchange_list)]
    df_exchange_volume_rank = df_volume_history_agg.groupby('exchange').agg(volumeUSD=('volumeUSD', 'sum'))
    df_exchange_volume_rank = df_exchange_volume_rank.sort_values('volumeUSD', ascending=False).reset_index()
    print(df_exchange_volume_rank)

    return df_exchange_volume_rank


# (agg) exchange total volume history + asset volume proportion
def exchange_volume_history_total(exchange_list, start, end):
    df_volume_history_agg = report_data.exchange_volume_history(start=start, end=end)
    df_volume_history_agg = df_volume_history_agg.loc[df_volume_history_agg['exchange'].isin(exchange_list)]
    df_volume_history_total = df_volume_history_agg.groupby('timestamp').agg(volumeUSD=('volumeUSD', 'sum')).reset_index()
    df_volume_history_total['datetime'] = df_volume_history_total['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
    print(df_volume_history_total)
    return df_volume_history_total


def asset_volume_history_agg(exchange_list, asset_list, start, end):
    df_asset_volume_history_agg = pd.DataFrame()
    for i in exchange_list:
        df_asset_volume_history_in_exchange = pd.DataFrame()

        for j in asset_list:
            result = report_data.asset_volume_history(exchange=i, asset=j, start=start, end=end)
            print(result)
            if len(result) > 0:
                df_asset_volume_history_in_exchange = pd.concat([df_asset_volume_history_in_exchange, result], ignore_index=True)

        print(df_asset_volume_history_in_exchange)
        df_asset_volume_history_agg = pd.concat([df_asset_volume_history_agg, df_asset_volume_history_in_exchange], ignore_index=True)

    df_asset_volume_history_agg = df_asset_volume_history_agg.groupby(['timestamp', 'asset']).sum().reset_index()
    df_asset_volume_history_agg = df_asset_volume_history_agg.sort_values(['timestamp', 'volumeUSD'], ascending=False)
    df_asset_volume_history_agg['datetime'] = df_asset_volume_history_agg['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
    print(df_asset_volume_history_agg)

    return df_asset_volume_history_agg


def funding_rate_history_agg(exchange, pair_list, start, end):
    df_funding_rate_agg = pd.DataFrame()
    for i in pair_list:
        result = report_data.funding_rate_history(exchange=exchange, pair=i, start=start, end=end)
        result['pair'] = i
        df_funding_rate_agg = pd.concat([df_funding_rate_agg, result], ignore_index=True)
    df_funding_rate_agg['datetime'] = df_funding_rate_agg['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
    print(df_funding_rate_agg)

    return df_funding_rate_agg


def open_interest_history_agg(exchange, pair_list, start, end):
    result = report_data.open_interest(exchange=exchange, pair=','.join(pair_list), start=start, end=end)
    result['datetime'] = result['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
    print(result)

    return result


def liquidation_history(exchange, pair_list, start, end):
    df_liquidation_history_agg = pd.DataFrame()
    for i in pair_list:
        result = report_data.liquidation_history(exchange=exchange, pair=i, start=start, end=end)
        result['datetime'] = result['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
        result['pair'] = i
        print(result)
        df_liquidation_history_agg = pd.concat([df_liquidation_history_agg, result], ignore_index=True)
    print(df_liquidation_history_agg)

    return df_liquidation_history_agg

# long-short ratio history
def long_short_ratio_history(exchange, pair_list, start, end):
    df_ls_ratio_history_agg = pd.DataFrame()
    for i in pair_list:
        result = report_data.long_short_ratio_history(exchange=exchange, pair=i, start=start, end=end)
        result['datetime'] = result['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
        result['pair'] = i
        print(result)
        df_ls_ratio_history_agg = pd.concat([df_ls_ratio_history_agg, result], ignore_index=True)
    print(df_ls_ratio_history_agg)

    return df_ls_ratio_history_agg


def nvt_ratio_history(asset_list, start, end):

    df_nvt_agg = pd.DataFrame()
    for i in asset_list:
        result = report_data.nvt_rate_history(asset=i, start=start, end=end)
        print(result)
        result['asset'] = i
        df_nvt_agg = pd.concat([df_nvt_agg, result], ignore_index=True)

    return df_nvt_agg


def ethereum_gas_fee_in_usd(start, end):
    result = report_data.gas_fee_eth(start=start, end=end)
    print(result)

    return result


# # (one) exchange total volume history + asset volume proportion
# exchange_list = ['binance', 'gdax', 'kraken', 'okex', 'bitfinex', 'bitstamp', 'mexc', 'gemini', 'bybit', 'huobi']
# asset_list = ['btc', 'eth', 'bnb', 'xrp', 'ada', 'doge', 'matic', 'sol', 'dot', 'shib', 'ltc', 'trx', 'avax', 'uni']
# start = datetime(2022, 12, 1).timestamp()
# end = datetime(2022, 12, 31).timestamp()
#
# result = report_data.exchange_volume_history(start=start, end=end)
# for i in exchange_list:
#     # total volume history
#     vol_history = result.loc[result['exchange'] == i]
#     vol_history = vol_history.sort_values('timestamp', ascending=True)
#     print(vol_history)
#     vol_history.to_csv(f'vol_history_{i}.csv')
#
# for i in exchange_list:
#     # asset volume proportion
#     df_asset_volume_history_in_exchange = pd.DataFrame()
#     for j in asset_list:
#         result = report_data.asset_volume_history(exchange=i, asset=j, start=start, end=end)
#         print(result)
#         if len(result) > 0:
#             df_asset_volume_history_in_exchange = pd.concat([df_asset_volume_history_in_exchange, result], ignore_index=True)
#
#     print(df_asset_volume_history_in_exchange)
#     df_asset_volume_history_in_exchange.to_csv(f'asset_volume_proportion_history_{i}.csv')