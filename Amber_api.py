import requests
import io
import time
import pandas as pd

class amber_api():
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://web3api.io/api/v2'


    def historical_price(self, asset):
        # https://docs.amberdata.io/reference/spot-prices-assets-historical
        # 7days
        url = self.base_url + f"/market/spot/prices/assets/{asset}/historical/?startDate={int(time.time())-86400*7}&timeInterval=d&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def asset_ranking_by_marketcap_latest(self, size):
        # https://docs.amberdata.io/reference/market-rankings
        url = f"https://web3api.io/api/v2/market/rankings/latest?direction=descending&sortType=marketCap&page=0&{size}=300&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def asset_volume_history(self, exchange, asset, start, end):
        # https: // docs.amberdata.io / reference / market - metrics - exchanges - assets - volumes - historical
        url = f"https://web3api.io/api/v2/market/metrics/exchanges/{exchange}/assets/volumes/historical?asset={asset}&quoteVolume=false&direction=desc&startDate={int(start)}&endDate={int(end)}&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def exchange_volume_history(self, start, end):
        # https: // docs.amberdata.io / reference / market - metrics - exchanges - volumes - historical
        url = f"https://web3api.io/api/v2/market/metrics/exchanges/volumes/historical?direction=descending&startDate={int(start)}&endDate={int(end)}&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def pair_volume_history(self):
        pair = 'btc_usd'
        exchange = 'bitfinex'
        end = int(time.time())
        start = end - 30 * 86400
        url = f"https://web3api.io/api/v2/market/metrics/exchanges/{exchange}/pairs/volumes/historical?pair={pair}&direction=desc&startDate={int(start)}&endDate={int(end)}&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": "UAK3484b3d443f4b50d3a915a616d24c66c"
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        print(df)

    def funding_rate_history(self, exchange, pair, start, end):
        url = f"https://web3api.io/api/v2/market/futures/funding-rates/{pair}/historical?exchange={exchange}&startDate={int(start)}&endDate={int(end)}&timeInterval=hours&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def funding_rate_history_continuous(self, pair, start, end):
        if end - start > 86400:
            df = pd.DataFrame()
            end_ = start + 86400*1000
            while end_ < end:
                print(start, end_)
                df_ = self.funding_rate_history(pair=pair, start=int(start), end=end_)
                df = pd.concat([df, df_], ignore_index=True)

                start = end_
                end_ += 86400*1000

            if end_ > end:
                df_ = self.funding_rate_history(pair=pair, start=start, end=end)
                df = pd.concat([df, df_], ignore_index=True)

            print(df)

        else:
            df = self.funding_rate_history(pair, start, end)
            print(df)

        return df

    def open_interest(self, exchange, pair, start, end):
        # https: // docs.amberdata.io / reference / batch - historical - ent - 1
        url = f"https://web3api.io/api/v2/market/futures/open-interest/exchange/{exchange}/historical?instrument={pair}&startDate={int(start)}&endDate={int(end)}&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def liquidation_history(self, exchange, pair, start, end):
        # https: // docs.amberdata.io / reference / futures - liquidations - historical
        url = f"https://web3api.io/api/v2/market/futures/liquidations/{pair}/historical?exchange={exchange}&startDate={int(start)}&endDate={int(end)}&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def long_short_ratio_history(self, exchange, pair, start, end):
        # https://docs.amberdata.io/reference/futures-long-short-ratio-historical
        url = f"https://web3api.io/api/v2/market/futures/long-short-ratio/{pair}/historical?exchange={exchange}&timeFrame=1h&startDate={int(start)}&endDate={int(end)}&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def nvt_rate_history(self, asset, start, end):
        # https://docs.amberdata.io/reference/market-asset-metrics-historical-nvt-pro
        url = f"https://web3api.io/api/v2/market/metrics/{asset}/historical/nvt?startDate={int(start)}&endDate={int(end)}&timeFrame=1d&format=csv"

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def gas_fee_eth(self, start, end):
        url = f"https://web3api.io/api/v2/transactions/metrics/historical?startDate={int(start)}&endDate={int(end)}&timeFrame=1d&format=csv"

        headers = {
            "accept": "application/json",
            "x-amberdata-blockchain-id": "ethereum-mainnet",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.text))

        return df