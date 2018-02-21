import csv
import traceback
import os
import math
import json
import pandas as pd
from datetime import datetime, timedelta
from future.moves.urllib.parse import urlencode
import requests

# global settings
exchanges = {
    'ZAIF': 'zaif',
    'BF': 'bitflyer',
}
tx_types = {
    'BID': '売り',
    'ASK': '買い',
    'RECEIVE': '受取',
    'WITHDRAW': '出金',
    'DEPOSIT': '預入',
    'FEE': '手数料',
    'SEND': '外部送付',
    'PURCHASE': '購入',
    'SELF': '自己',
}
csv_types = {
    'ZAIF_TRADE': 'zaif_trade',
    'ZAIF_DEPOSIT': 'zaif_deposit',
    'ZAIF_TOKEN_DEPOSIT': 'zaif_token_deposit',
    'ZAIF_WITHDRAW': 'zaif_withdraw',
    'ZAIF_PURCHASE': 'zaif_purchase',
    'ZAIF_BONUS': 'zaif_bonus',
    'BITFLYER': 'bitflyer',
}

class TradeHistory:
    columns = [
        'market', 'type', 'price', 'cost', 'amount', 'time', 'exchange',
    ]
    bf_currencies = [
        'BTC', 'ETH', 'ETC', 'LTC', 'BCH', 'MONA', 'JPY',
    ]
    def __init__(self):
        self.data = pd.DataFrame(columns=TradeHistory.columns)
    def __getattr__(self, name):
        # wrapping pandas dataframe
        return self.data[name]
    def head(self, rows = 5):
        return self.data.head(rows)
    def tail(self, rows = 5):
        return self.data.tail(rows)
    def set_data(self, data, type):
        self.data = self.format_data(data, type)
    def append_data(self, data, type, currency=''):
        self.data = self.data.append(self.format_data(data, type, currency))
    def format_data(self, data, type, currency = ''):
        if type == csv_types['ZAIF_TRADE']:
            data.drop('コメント', axis=1, inplace=True)
            if 'ボーナス円' in data.columns:
                data.drop('ボーナス円', axis=1, inplace=True)
            data.rename(columns = {
                'マーケット': 'market',
                '取引種別': 'type',
                '価格': 'price',
                '取引手数料': 'cost',
                '数量': 'amount',
                '日時': 'time',
            }, inplace=True)
            data = pd.concat([
                data,
                pd.DataFrame(
                    [exchanges['ZAIF'] for i in range(len(data.index))],
                    columns=['exchange']
                ),
            ], axis=1)
            return data
        else:
            # for row-wise process
            if type == csv_types['BITFLYER']:
                def rule(row, currency):
                    if row['通貨'].endswith('/JPY'):
                        coin = row['通貨'].split('/')[0]
                        market = coin.lower() + '_jpy'
                    elif row['通貨'] in TradeHistory.bf_currencies:
                        coin = row['通貨']
                        market = coin.lower()
                    return {
                        'time': row['取引日時'],
                        'type': row['取引種別'],
                        'price': row['価格'],
                        'market': market,
                        'amount': abs(row[coin]),
                        'cost': abs(row.get('手数料(' + coin + ')', 0)),
                        'exchange': exchanges['BF'],
                    }
            elif type == csv_types['ZAIF_DEPOSIT']:
                rule = lambda row, currency: {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': currency,
                    'type': tx_types['DEPOSIT'],
                    'price': 0,
                    'cost': 0,
                    'exchange': exchanges['ZAIF'],
                }
            elif type == csv_types['ZAIF_TOKEN_DEPOSIT']:
                rule = lambda row, currency: {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': row['トークン'].lower(),
                    'type': tx_types['DEPOSIT'],
                    'price': 0,
                    'cost': 0,
                    'exchange': exchanges['ZAIF'],
                }
            elif type == csv_types['ZAIF_PURCHASE']:
                rule = lambda row, currency: {
                    'time': row['日時'],
                    'amount': row['数量'],
                    'market': row['通貨'],
                    'type': tx_types['PURCHASE'],
                    'price': 0,
                    'cost': row['価格'],
                    'exchange': exchanges['ZAIF'],
                }
            elif type == csv_types['ZAIF_BONUS']:
                rule = lambda row, currency: {
                    'time': row['支払日時'],
                    'amount': row['支払ボーナス'],
                    'market': 'jpy',
                    'type': tx_types['RECEIVE'],
                    'price': 0,
                    'cost': 0,
                    'exchange': exchanges['ZAIF'],
                }
            elif type == csv_types['ZAIF_WITHDRAW']:
                rule = lambda row, currency: {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': currency,
                    'type': tx_types['WITHDRAW'],
                    'price': 0,
                    'cost': row['手数料'] ,
                    'exchange': exchanges['ZAIF'],
                }
            return pd.DataFrame(
                [rule(row, currency) for index, row in data.iterrows()],
                columns=TradeHistory.columns
            )


class ProfitCalculator:
    # variation of coins
    coins = [
        'jpy', 'btc', 'bch', 'eth', 'mona', 'xem',
        'zaif', 'xcp', 'bitcrystals', 'sjcx', 'fscc', 'pepecash', 'cicc',
        'ncxc', 'jpyz', 'erc20.cms', 'mosaic.cms',
    ]
    # Timestamps for hard forks
    hf_timestamps = {
        'bch': {
            'src': 'btc',
            'timestamp': 1501611180,
        },
    }
    # Endpoint of Zaif API
    zaif_api = 'https://zaif.jp/zaif_chart_api/v1/history'

    def set_hf_exceptions(self, hf_exceptions):
        self.hf_exceptions = hf_exceptions

    def __init__(self, initial = {}):
        # Amount of every coin
        self.coins = {k: 0 for k in ProfitCalculator.coins}
        # Acquisition cost of every coin
        self.acq_costs = {k: 0 for k in ProfitCalculator.coins}

        self.hf_flags = {
            'bch': False,
        }

        self.profit = {
            2017: 0,
            2018: 0,
        }
        self.deposit_jpy = 0
        self.last_tx_time = None
        self.trade = TradeHistory()
        self.hf_exceptions = None

    def print_status(self):
        for year in self.profit.keys():
            print('-- ', year,' ---------------------------------')
            for key, row in self.coins.items():
                if self.coins[key] > 0:
                    print(key.upper() + ':', round(self.coins[key], 8))
                    print(key.upper() + ' Acquisition cost:', round(self.acq_costs[key], 8))
            print()
            print('As of:', self.last_tx_time)
            print('Profits:', round(self.profit[year]))
            print('Spent:', round(self.deposit_jpy))
            print('Acquisition cost:', sum([self.coins[c] * self.acq_costs[c] for c in ProfitCalculator.coins]))
            print()

    def get_coin_type(self, market):
        return market.split('_')[0]

    def load_history(self, data_list):
        for key, value in data_list.items():
            for item in value:
                if os.path.exists(item['path']):
                    self.trade.append_data(pd.read_csv(item['path']), key, item.get('currency', ''))
        self.trade.data['time'] = pd.to_datetime(self.trade.data['time'])
        self.trade.data = self.trade.data.sort_values(by='time', ascending=True)
        self.trade.data = self.trade.data.reset_index(drop=True)

    def get_fair_value(self, time, symbol):
        time = time - timedelta(seconds=time.second)
        params = {
            'symbol': symbol.upper(),
            'resolution': '1',
            'from': int(time.timestamp()),
            'to': int(time.timestamp()),
        }
        encoded_params = urlencode(params)
        response = requests.get(ProfitCalculator.zaif_api, params=params)
        if response.status_code != 200:
            raise Exception('return status code is {}'.format(response.status_code))
        data = json.loads(response.json())
        count = data['data_count']
        if (count != 0):
            data = pd.DataFrame.from_dict(data['ohlc_data'])
        return data['close'][0]

    def bid(self, row):
        # Unit of fee in bid is jpy or btc
        coin_type = self.get_coin_type(row['market'])
        if row['market'].endswith('_jpy'):
            self.profit[row['time'].year] += (row['price'] - math.ceil(self.acq_costs[coin_type])) * row['amount']
            if row['exchange'] == exchanges['ZAIF']:
                self.coins['jpy'] += row['price'] * row['amount'] - row['cost']
            elif row['exchange'] == exchanges['BF']:
                self.coins['jpy'] += math.floor(row['price'] * row['amount']) - row['cost']
        elif row['market'].endswith('_btc'):
            # Call an API to get a fair value at this moment.
            btc_fair_value = self.get_fair_value(row['time'], 'BTC_JPY')
            coin_type = self.get_coin_type(row['market'])
            alt_fair_value = self.get_fair_value(row['time'], coin_type.upper() + '_JPY')
            self.profit[row['time'].year] += (alt_fair_value - math.ceil(self.acq_costs[coin_type])) * row['amount']

            new_coins = row['price'] * row['amount']
            if self.coins['btc'] == 0:
                # If this was first time to have BTC
                self.coins['btc'] = new_coins - row['cost']
                self.acq_costs['btc'] = btc_fair_value
            else:
                former_cost = self.acq_costs['btc'] * self.coins['btc']
                cost = former_cost + btc_fair_value * new_coins
                self.coins['btc'] += new_coins - row['cost']
                self.acq_costs['btc'] = cost / self.coins['btc']
        self.coins[coin_type] -= row['amount']

    def ask(self, row):
        # Unit of fee in ask is buying currency
        coin_type = self.get_coin_type(row['market'])
        if row['market'].endswith('_jpy'):
            jpy = row['amount'] * row['price']
            if row['exchange'] == exchanges['ZAIF']:
                self.coins['jpy'] -= jpy
            elif row['exchange'] == exchanges['BF']:
                self.coins['jpy'] -= math.ceil(jpy)
            if self.coins[coin_type] == 0:
                # if this was the first time to by this type of coin
                self.coins[coin_type] = row['amount'] - row['cost']
                self.acq_costs[coin_type] = row['price']
            else:
                former_cost = self.acq_costs[coin_type] * self.coins[coin_type] 
                cost = former_cost + jpy
                self.coins[coin_type] += row['amount'] - row['cost']
                self.acq_costs[coin_type] = cost / self.coins[coin_type]
        elif row['market'].endswith('_btc'):
            self.coins['btc'] -= row['price'] * row['amount']
            # fair value at this moment
            fair_value = self.get_fair_value(row['time'], coin_type.upper() + '_JPY')
            if self.coins[coin_type] == 0:
                # if this was the first time to by this type of coin
                self.coins[coin_type] = row['amount'] - row['cost']
                self.acq_costs[coin_type] = fair_value
            else:
                former_cost = self.acq_costs[coin_type] * self.coins[coin_type]
                cost = former_cost + fair_value * row['amount']
                self.coins[coin_type] += row['amount'] - row['cost']
                self.acq_costs[coin_type] = cost / self.coins[coin_type]

    def purchase(self, row):
        self.deposit_jpy += row['cost']
        coin_type = row['market']
        if self.acq_costs[coin_type] == 0:
            # if not coin yet
            self.coins[coin_type] = row['amount']
            self.acq_costs[coin_type] = row['cost'] / self.coins[coin_type]
        else:
            # if you already have coins, take moving average.
            former_cost = self.acq_costs[coin_type] * self.coins[coin_type]
            new_cost = former_cost + row['cost']
            self.coins[coin_type] += row['amount']
            self.acq_costs[coin_type] = new_cost / self.coins[coin_type]

    def deposit(self, row):
        self.coins[row['market']] += row['amount'] - row['cost']
        if row['market'] == 'jpy':
            self.deposit_jpy += row['amount'] - row['cost']

    def withdraw(self, row):
        self.coins[row['market']] -= row['amount'] + row['cost']
        if row['market'] == 'jpy':
            self.deposit_jpy -= row['amount'] + row['cost']

    def send(self, row):
        self.withdraw(row)

    def receive(self, row):
        self.coins[row['market']] += row['amount'] - row['cost']

    def fee(self, row):
        self.coins[row['market']] -= row['amount']

    def check_hard_fork(self, row):
        for coin, flag in self.hf_flags.items():
            if row['time'].timestamp() > ProfitCalculator.hf_timestamps[coin]['timestamp'] and not flag:
                self.coins[coin] = self.coins[ProfitCalculator.hf_timestamps[coin]['src']]
                if coin in self.hf_exceptions:
                    self.coins[coin] -= self.hf_exceptions[coin]
                self.hf_flags[coin] = True

    def calculate(self, num_of_tx = -1):
        inv_tx_types = {v:k for k, v in tx_types.items()}
        for index, row in self.trade.data.iterrows():
            self.check_hard_fork(row)
            # Reflection
            try:
                key = inv_tx_types[row['type']].lower()
            except KeyError as e:
                traceback.print_exc()
                raise Exception('Unsupported transaction')
            if hasattr(self, key):
                action = getattr(self, key)
                if callable(action):
                    action(row)
                    self.last_tx_time = row['time']
            # self.print_status()
            if index == num_of_tx:
                break

    def add_credit(self, credit_profits):
        self.coins['jpy'] += sum(credit_profits)

if __name__ == "__main__":
    # List of csv files for transaction history
    data_list = {
        csv_types['ZAIF_TRADE']: [
            { 'path': 'trade_log.csv' },
            { 'path': 'token_trade_log.csv' },
        ],
        csv_types['ZAIF_DEPOSIT']: [
            { 'path': 'jpy_deposit.csv', 'currency': 'jpy' },
            { 'path': 'btc_deposit.csv', 'currency': 'btc' },
            { 'path': 'mona_deposit.csv', 'currency': 'mona' },
        ],
        csv_types['ZAIF_TOKEN_DEPOSIT']: [
            { 'path': 'token_deposit.csv' }
        ],
        csv_types['ZAIF_WITHDRAW']: [
            { 'path': 'btc_withdraw.csv', 'currency': 'btc' },
            { 'path': 'bch_withdraw.csv', 'currency': 'bch' },
            { 'path': 'eth_withdraw.csv', 'currency': 'eth' },
            { 'path': 'mona_withdraw.csv', 'currency': 'mona' },
            { 'path': 'token_withdraw_ZAIF.csv', 'currency': 'zaif' },
        ],
        csv_types['ZAIF_PURCHASE']: [
            { 'path': 'purchase.csv' },
        ],
        csv_types['ZAIF_BONUS']: [
            { 'path': 'obtain_bonus.csv' },
        ],
        csv_types['BITFLYER']: [
            { 'path': 'TradeHistory.csv' },
        ]
    }
    # profits in credit trade in JPY
    credit_profits = [
        0.183113
    ]
    hf_exceptions = {
        'bch': 0.01562526
    }

    cal = ProfitCalculator()
    cal.load_history(data_list)
    cal.set_hf_exceptions(hf_exceptions)
    cal.calculate()
    cal.add_credit(credit_profits)
    cal.print_status()

