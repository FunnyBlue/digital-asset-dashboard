import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime


def price_change(df, timeframe):
    df_pc = df
    fig = go.Figure(go.Bar(
                x=df_pc[f'changeInPrice{timeframe[0].upper()+timeframe[1:]}'],
                y=df_pc['symbol'],
                orientation='h',))
    fig.update_layout(
        title=f"Price Change {timeframe[0].upper()+timeframe[1:]}",
        xaxis_title="Change(%)",
        yaxis_title="Asset",
        yaxis={'categoryorder': 'total ascending'}
    )
    #fig.show()
    
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/Price Change {timeframe[0].upper()+timeframe[1:]}.html')


def asset_volume_rank(df, timeframe):
    df_exchange_volume = df

    fig = go.Figure(go.Bar(
                x=df_exchange_volume['volumeUSD'],
                y=df_exchange_volume['asset'],
                orientation='h'))
    fig.update_layout(
        title=f"Asset Trade Volume Rank in {timeframe} Days (Top 10 Exchange)",
        xaxis_title="Volume in USD",
        yaxis_title="Asset",
        yaxis={'categoryorder': 'total ascending'}
    )
    #fig.show()
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/Asset Trade Volume Rank in {timeframe} Days (Top 10 Exchange).html')


# (agg) exchange total volume history + asset volume proportion
def exchange_volume_history_total(df):
    df_volume_history_agg = df

    fig = go.Figure(go.Scatter(x=df_volume_history_agg['datetime'], y=df_volume_history_agg['volumeUSD'], fill='tozeroy'))
    fig.update_layout(
        title=f"Top 10 Exchange Trade Volume History",
        xaxis_title="Datetime",
        yaxis_title="Volume in USD",
    )
    #fig.show()
    
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/Top 10 Exchange Trade Volume History.html')


def asset_volume_proportion_history(df):
    df_volume_proportion_history = df

    fig = px.area(df_volume_proportion_history, x="datetime", y="volumeUSD", color="asset", groupnorm='fraction')
    fig.update_layout(
        title=f"Top 10 Exchange Spot Volume Proportion",
        xaxis_title="Datetime",
    )
    #fig.show()
    
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/Top 10 Exchange Spot Volume Proportion.html')


def exchange_volume_rank(df, timeframe):
    df_exchange_volume_rank = df

    fig = go.Figure([go.Bar(x=df_exchange_volume_rank['exchange'], y=df_exchange_volume_rank['volumeUSD'])])
    fig.update_layout(
        title=f"Exchange Volume Rank in {timeframe}",
        xaxis_title="Exchange",
        yaxis_title="Volume in USD",
    )
    #fig.show()
    
    
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/Exchange Volume Rank in {timeframe}.html')


def funding_rate_history(df, exchange):
    df_funding_rate_history = df

    fig = px.line(df_funding_rate_history, x="datetime", y="fundingRate", color='pair')
    fig.update_layout(
        title=f"{exchange[0].upper()+exchange[1:]} Funding Rate History",
        xaxis_title="Datetime",
        yaxis_title="Rate",

    )
    #fig.show()
    
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/{exchange[0].upper()+exchange[1:]} Funding Rate History.html')


def open_interest_history(df, exchange, pair_list):
    df_open_interest_history_agg = df

    for i in pair_list:
        df = df_open_interest_history_agg.loc[df_open_interest_history_agg['instrument'] == i]
        fig = px.line(df, x="datetime", y="value")
        fig.update_layout(
            title=f"{i} {exchange[0].upper()+exchange[1:]} Open Interest History",
            xaxis_title="Datetime",
            yaxis_title="Volume(base asset)",
        )
        #fig.show()
        
        fig.show("png",config={'scrollZoom': True})

        fig.write_html(f'chart/{i} {exchange[0].upper()+exchange[1:]} Open Interest History.html')


def liquidation_history(df, exchange, pair_list):
    df_liquidation_history_agg = df

    for i in pair_list:
        df = df_liquidation_history_agg.loc[df_liquidation_history_agg['pair'] == i]
        fig = px.scatter(df, x="datetime", y="originalQuantity", color='positionType')
        fig.update_layout(
            title=f"{i} {exchange[0].upper()+exchange[1:]} Liquidation History",
            xaxis_title="Datetime",
            yaxis_title="Volume(base token)",
        )
        #fig.show()
        
        
        fig.show("png",config={'scrollZoom': True})

        fig.write_html(f'chart/{i} {exchange[0].upper()+exchange[1:]} Liquidation History.html')


def long_short_ratio_history(df, exchange):
    df_long_short_history_agg = df

    fig = px.line(df_long_short_history_agg, x="datetime", y="ratio", color='pair')
    fig.update_layout(
        title=f"{exchange[0].upper()+exchange[1:]} Futures Long Short Ratio History",
        xaxis_title="Datetime",
        yaxis_title="Ratio(long/short account)",
    )
    #fig.show()
    
    
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/{exchange[0].upper()+exchange[1:]} Futures Long Short Ratio History.html')


def nvt_ratio_history(df):
    df_nvt_ratio = df

    fig = px.line(df_nvt_ratio, x="timestamp", y="nvt_ratio", color='asset')
    fig.update_layout(
        title="NVT Ratio",
        xaxis_title="Datetime",
        yaxis_title="NVT Ratio",
    )
    #fig.show()
    
    
    fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/NVT Ratio.html')


def ethereum_gas_fee_in_usd(df):
    df_gas_fee = df

    fig = px.line(df_gas_fee, x="timestamp", y="feesAverageUSD")
    fig.update_layout(
        title="Ethereum Average Gas Fee",
        xaxis_title="Datetime",
        yaxis_title="USD",
    )
    fig.show()
    
    
    #fig.show("png",config={'scrollZoom': True})

    fig.write_html(f'chart/Ethereum Average Gas Fee.html')


# # (one) exchange total volume history + asset volume proportion
# exchange_list = ['binance', 'gdax', 'kraken', 'okex']
# for i in exchange_list:
#     df_volume_history = pd.read_csv(f'vol_history_{i}.csv', index_col=0).sort_values('timestamp', ascending=True).reset_index(drop=True)
#     df_volume_history['datetime'] = df_volume_history['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
#     print(df_volume_history)
#     fig = go.Figure(go.Scatter(x=df_volume_history['datetime'], y=df_volume_history['volumeUSD'], fill='tozeroy'))
#
#     fig.update_layout(
#         title=f"{i[0].upper()}{i[1:]} Trade Volume History in 30 days",
#         xaxis_title="Datetime",
#         yaxis_title="Volume in USD",
#     )
#
#     fig.show()
#
#     df_volume_proportion_exchange = pd.read_csv(f'asset_volume_proportion_history_{i}.csv',index_col=0)
#     df_volume_proportion_exchange['datetime'] = df_volume_proportion_exchange['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
#     print(df_volume_proportion_exchange)
#
#     fig = px.area(df_volume_proportion_exchange, x="datetime", y="volumeUSD", color="asset", groupnorm='fraction')
#     fig.update_layout(
#         title=f"{i[0].upper()}{i[1:]} Spot Volume Proportion",
#         xaxis_title="Datetime",
#     )
#     fig.show()