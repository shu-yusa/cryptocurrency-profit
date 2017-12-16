import csv
import os
import math
import json
import pandas as pd
from datetime import datetime, timedelta
from future.moves.urllib.parse import urlencode
import requests

class TradeHistory:
    columns = [
        'market', 'type', 'price', 'cost', 'amount', 'time', 'exchange',
    ]
    def __init__(self):
        self.data = pd.DataFrame(columns=TradeHistory.columns)
    def head(self, rows = 5):
        return self.data.head(rows)
    def tail(self, rows = 5):
        return self.data.tail(rows)
    def format_data(self, data, type, currency = ''):
        if type == 'zaif_deposit':
            data = data.sort_values(by='日時', ascending=True)
            data['日時'] = pd.to_datetime(data['日時'])
            data2 = pd.DataFrame(columns=TradeHistory.columns)
            for index, row in data.iterrows():
                d = {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': currency,
                    'type': '預入',
                    'price': 0,
                    'cost': 0,
                    'exchange': 'zaif',
                }
                data2 = data2.append(pd.DataFrame(d, index=[0]), ignore_index=True)
            return data2
        elif type == 'zaif_token_deposit':
            data = data.sort_values(by='日時', ascending=True)
            data['日時'] = pd.to_datetime(data['日時'])
            data2 = pd.DataFrame(columns=TradeHistory.columns)
            for index, row in data.iterrows():
                d = {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': row['トークン'].lower(),
                    'type': '預入',
                    'price': 0,
                    'cost': 0,
                    'exchange': 'zaif',
                }
                data2 = data2.append(pd.DataFrame(d, index=[0]), ignore_index=True)
            return data2
        elif type == 'zaif_purchase':
            data = data.sort_values(by='日時', ascending=True)
            data['日時'] = pd.to_datetime(data['日時'])
            data2 = pd.DataFrame(columns=TradeHistory.columns)
            for index, row in data.iterrows():
                d = {
                    'time': row['日時'],
                    'amount': row['数量'],
                    'market': row['通貨'],
                    'type': '購入',
                    'price': 0,
                    'cost': row['価格'],
                    'exchange': 'zaif',
                }
                data2 = data2.append(pd.DataFrame(d, index=[0]), ignore_index=True)
            return data2
        elif type == 'zaif_bonus':
            data = data.sort_values(by='支払日時', ascending=True)
            data['支払日時'] = pd.to_datetime(data['支払日時'])
            data2 = pd.DataFrame(columns=TradeHistory.columns)
            for index, row in data.iterrows():
                d = {
                    'time': row['支払日時'],
                    'amount': row['支払ボーナス'],
                    'market': 'jpy',
                    'type': '受取',
                    'price': 0,
                    'cost': 0,
                    'exchange': 'zaif',
                }
                data2 = data2.append(pd.DataFrame(d, index=[0]), ignore_index=True)
            return data2
        elif type == 'zaif_withdraw':
            data = data.sort_values(by='日時', ascending=True)
            data['日時'] = pd.to_datetime(data['日時'])
            data2 = pd.DataFrame(columns=TradeHistory.columns)
            for index, row in data.iterrows():
                d = {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': currency,
                    'type': '出金',
                    'price': 0,
                    'cost': row['手数料'] ,
                    'exchange': 'zaif',
                }
                data2 = data2.append(pd.DataFrame(d, index=[0]), ignore_index=True)
            return data2
        elif type == 'zaif':
            data = data.sort_values(by='日時', ascending=True)
            data['日時'] = pd.to_datetime(data['日時'])
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
            data = pd.concat([data, pd.DataFrame(['zaif' for i in range(len(data.index))], columns=['exchange'])], axis=1)
            return data
        elif type == 'bitflyer':
            data = data.sort_values(by='取引日時', ascending=True)
            data['取引日時'] = pd.to_datetime(data['取引日時'])
            data2 = pd.DataFrame(columns=TradeHistory.columns)
            for index, row in data.iterrows():
                d = {
                    'time': row['取引日時'],
                    'type': row['取引種別'],
                    'price': row['価格'],
                    'exchange': 'bitflyer',
                }
                if row['通貨'] == 'BTC':
                    # 預入 or 受け取り or 外部送付 or 手数料
                    d['market'] = 'btc'
                    d['amount'] = abs(row['BTC'])
                    d['cost'] = abs(row['手数料(BTC)'])
                elif row['通貨'] == 'MONA':
                    # 預入 or 外部送付 or 手数料
                    d['market'] = 'mona'
                    d['amount'] = abs(row['MONA'])
                    d['cost'] = abs(row['手数料(MONA)'])
                elif row['通貨'] == 'JPY':
                    d['market'] = 'jpy'
                    d['amount'] = abs(row['JPY'])
                    d['cost'] = 0
                elif row['通貨'] == 'BTC/JPY':
                    d['market'] = 'btc_jpy'
                    d['amount'] = abs(row['BTC'])
                    d['cost'] = abs(row['手数料(BTC)'])
                elif row['通貨'] == 'MONA/JPY':
                    d['market'] = 'mona_jpy'
                    d['amount'] = abs(row['MONA'])
                    d['cost'] = abs(row['手数料(MONA)'])
                data2 = data2.append(pd.DataFrame(d, index=[0]), ignore_index=True)
            return data2
    def set_data(self, data, type):
        self.data = self.format_data(data, type)
    def append_data(self, data, type, currency=''):
        self.data = self.data.append(self.format_data(data, type, currency))
    def __getattr__(self, name):
        return self.data[name]


class ProfitCalculator:
    # variation of coins
    coins = [
        'jpy', 'btc', 'bch', 'eth', 'mona', 'xem',
        'zaif', 'xcp', 'bitcrystals', 'sjcx', 'fscc', 'pepecash', 'cicc',
        'ncxc', 'jpyz', 'erc20.cms', 'mosaic.cms',
    ]
    # Timestamps for hard forks
    hf_timestamps = {
        'bch': 1501611180,
    }
    # Endpoint of Zaif API
    zaif_api = 'https://zaif.jp/zaif_chart_api/v1/history'
    def __init__(self, initial = {}):
        # Amount of every coin
        self.coins = {k: 0 for k in ProfitCalculator.coins}
        # Acquisition cost of every coin
        self.acq_costs = {k: 0 for k in ProfitCalculator.coins}

        self.hf_flags = {
            'bch': False,
        }

        self.profit = 0
        self.deposit_jpy = 0

        self.trade = TradeHistory()
    def print_status(self):
        print('--------------------------------------------------')
        for key, row in self.coins.items():
            if self.coins[key] > 0:
                print(key.upper() + ':', round(self.coins[key], 8))
                print(key.upper() + ' Acquisition cost:', round(self.acq_costs[key], 8))
        print()
        print('Profits:', round(self.profit))
        print('Spent:', round(self.deposit_jpy))
        print('Acquisition cost:', sum([self.coins[c] * self.acq_costs[c] for c in ProfitCalculator.coins]))
        print('--------------------------------------------------')
    #def purchase(self, coin_type, amount, cost):
    def purchase(self, row):
        self.deposit_jpy += row['cost']
        coin_type = row['market']
        if self.acq_costs[coin_type] == 0:
            # if not coin yet
            self.coins[coin_type] = row['amount']
            self.acq_costs[coin_type] = row['cost'] / self.coins[coin_type]
        else:
            # if you already have coins
            former_cost = self.acq_costs[coin_type] * sef.coins[coin_type]
            new_cost = former_cost + row['cost']
            self.coins[coin_type] += row['amount']
            self.acq_costs[coin_type] = new_cost / self.coins[coin_type]
    def get_coin_type(self, market):
        return market.split('_')[0]
    def load_history(self, data_list):
        for item in data_list['zaif_trade']:
            self.trade.append_data(pd.read_csv(item['path']), 'zaif')
        for item in data_list['zaif_deposit']:
            if os.path.exists(item['path']):
               self.trade.append_data(pd.read_csv(item['path']), 'zaif_deposit', item['currency'])
        for item in data_list['zaif_token_deposit']:
            if os.path.exists(item['path']):
                self.trade.append_data(pd.read_csv(item['path']), 'zaif_token_deposit')
        for item in data_list['zaif_withdraw']:
            if os.path.exists(item['path']):
                self.trade.append_data(pd.read_csv(item['path']), 'zaif_withdraw', item['currency'])
        for item in data_list['zaif_purchase']:
            if os.path.exists(item['path']):
                self.trade.append_data(pd.read_csv(item['path']), 'zaif_purchase')
        for item in data_list['zaif_bonus']:
            if os.path.exists(item['path']):
                self.trade.append_data(pd.read_csv(item['path']), 'zaif_bonus')
        for item in data_list['bitflyer']:
            if os.path.exists(item['path']):
                self.trade.append_data(pd.read_csv(item['path']), 'bitflyer')
        self.trade.data = self.trade.data.sort_values(by='time')
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
    def bid(self, row): # 売り
        # Unit of fee in bid is jpy or btc
        coin_type = self.get_coin_type(row['market'])
        if row['market'].endswith('_jpy'):
            # self.profit += row['price'] * row['amount'] - math.ceil(self.acq_costs[coin_type] * row['amount'])
            self.profit += (row['price'] - math.ceil(self.acq_costs[coin_type])) * row['amount']
            if row['exchange'] == 'zaif':
                self.coins['jpy'] += row['price'] * row['amount'] - row['cost']
            elif row['exchange'] == 'bitflyer':
                print('bid', math.floor(row['price'] * row['amount']))
                self.coins['jpy'] += math.floor(row['price'] * row['amount']) - row['cost']
        elif row['market'].endswith('_btc'):
            # Call an API to get a fair value at this moment.
            fair_value = self.get_fair_value(row['time'], 'BTC_JPY')
            new_coins = row['price'] * row['amount']
            if self.coins['btc'] == 0:
                # If this was first time to have BTC
                self.coins['btc'] = new_coins - row['cost']
                self.acq_costs['btc'] = fair_value
            else:
                former_cost = self.acq_costs['btc'] * self.coins['btc']
                cost = former_cost + fair_value * new_coins
                self.coins['btc'] += new_coins - row['cost']
                self.acq_costs['btc'] = cost / self.coins['btc']
        self.coins[coin_type] -= row['amount']
    def ask(self, row): # 買い
        # Unit of fee in ask is buying currency
        coin_type = self.get_coin_type(row['market'])
        if row['market'].endswith('_jpy'):
            jpy = row['amount'] * row['price']
            if row['exchange'] == 'zaif':
                self.coins['jpy'] -= jpy
            elif row['exchange'] == 'bitflyer':
                print('ask:', math.ceil(jpy))
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
    def deposit(self, row): # 預入
        coin_type = row['market']
        if coin_type == 'jpy':
            self.coins['jpy'] += row['amount']
            self.deposit_jpy += row['amount'] - row['cost']
        else:
            self.coins[coin_type] += row['amount'] - row['cost']
    def receive(self, row): # 受取
        coin_type = row['market']
        if coin_type == 'jpy':
            self.coins['jpy'] += row['amount']
        else:
            self.coins[coin_type] += row['amount'] - row['cost']
    def withdraw(self, row): # 出金
        coin_type = row['market']
        if coin_type == 'jpy':
            self.coins['jpy'] -= row['amount'] + row['cost']
            self.deposit_jpy -= row['amount'] + row['cost']
        else:
            self.coins[coin_type] -= row['amount'] + row['cost']
    def fee(self, row): # 手数料
        coin_type = row['market']
        if coin_type == 'jpy':
            self.coins['jpy'] -= row['amount']
        else:
            self.coins[coin_type] -= row['amount']
    def sending(self, row): # 外部送付
        coin_type = row['market']
        if coin_type == 'jpy':
            self.coins['jpy'] -= row['amount'] + row['cost']
            self.deposit_jpy -= row['amount'] + row['cost']
        else:
            self.coins[coin_type] -= row['amount'] + row['cost']
    def check_hard_fork(self, row):
        for coin, flag in self.hf_flags.items():
            if row['time'].timestamp() > ProfitCalculator.hf_timestamps[coin] and not flag:
                self.coins[coin] = self.coins['btc']
                self.hf_flags[coin] = True
    def calculate(self, num_of_tx = 0):
        n = 0
        for index, row in self.trade.data.iterrows():
            n = n + 1
            #print(row)
            self.check_hard_fork(row)
            if row['type'] == '売り':
                self.bid(row)
            elif row['type'] == '買い':
                self.ask(row)
            elif row['type'] == '受取':
                self.receive(row)
            elif row['type'] == '出金':
                self.withdraw(row)
            elif row['type'] == '預入':
                self.deposit(row)
            elif row['type'] == '手数料':
                self.fee(row)
            elif row['type'] == '外部送付':
                self.sending(row)
            elif row['type'] == '購入':
                self.purchase(row)
            # self.print_status()
            if n == num_of_tx:
                break

if __name__ == "__main__":
    # List of csv files for transaction history
    data_list = {
        'zaif_trade': [
            { 'path': 'trade_log.csv' },
            { 'path': 'token_trade_log.csv' },
        ],
        'zaif_deposit': [
            { 'path': 'jpy_deposit.csv', 'currency': 'jpy' },
            { 'path': 'btc_deposit.csv', 'currency': 'btc' },
            { 'path': 'mona_deposit.csv', 'currency': 'mona' },
        ],
        'zaif_token_deposit': [
            { 'path': 'token_deposit.csv' }
        ],
        'zaif_withdraw': [
            { 'path': 'btc_withdraw.csv', 'currency': 'btc' },
            { 'path': 'bch_withdraw.csv', 'currency': 'bch' },
            { 'path': 'mona_withdraw.csv', 'currency': 'mona' },
            { 'path': 'token_withdraw_ZAIF.csv', 'currency': 'zaif' },
        ],
        'zaif_purchase': [
            { 'path': 'purchase.csv' },
        ],
        'zaif_bonus': [
            { 'path': 'obtain_bonus.csv' },
        ],
        'bitflyer': [
            { 'path': 'TradeHistory.csv' },
        ]
    }

    cal = ProfitCalculator()
    cal.load_history(data_list)
    cal.calculate()
    cal.print_status()

