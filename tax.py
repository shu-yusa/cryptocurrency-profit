import csv
import math
import json
import pandas as pd
from datetime import datetime, timedelta
from future.moves.urllib.parse import urlencode
import requests

class TxData:
    pass

class ProfitCalculator:
    coins = [ 'btc', 'bch', 'eth', 'mona',
        'xem', 'zaif', 'pepecash', 'xcp', 'erc20.cms'
    ]
    zaif_api = 'https://zaif.jp/zaif_chart_api/v1/history'
    def __init__(self, initial = {}):
        self.coins = {k: 0 for k in ProfitCalculator.coins}
        self.acq_costs = {k: 0 for k in ProfitCalculator.coins}

        self.jpy = 0
        self.profit = 0
    def print_status(self):
        print('~~~~~~~~~~~~~~~~~~~~~~~~')
        print('BTC:', self.coins['btc'])
        print('BTC Acquisition cost:', self.acq_costs['btc'])
        print('MONA:', self.coins['mona'])
        print('MONA Acquisition cost:', self.acq_costs['mona'])
        print('Profits:', self.profit)
        print('Spent:', self.jpy)
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    def purchase(self, coin_type, amount, cost):
        if self.acq_costs[coin_type] == 0:
            # if not coin yet
            self.jpy += cost
            self.coins[coin_type] = amount
            self.acq_costs[coin_type] = cost / self.coins[coin_type]
        else:
            # if you already have coins
            former_cost = self.acq_costs[coin_type] * sef.coins[coin_type]
            new_cost = former_cost + cost
            self.jpy += cost
            self.coins[coin_type] += amount
            self.acq_costs[coin_type] = new_cost / self.coins[coin_type]
    def get_coin_type(self, market):
        return market.split('_')[0]
    def read_log(self):
        trade1 = pd.read_csv('trade_log.csv')
        trade2 = pd.read_csv('token_trade_log.csv')
        trade3 = pd.ExcelFile('TradeHistory.csv')
        trade = pd.concat([trade1, trade2])
        self.trade = trade.sort_values(by='日時', ascending=True)
        self.trade['日時'] = pd.to_datetime(self.trade['日時'])
    def calc_profit_crypt(self, data):
        pass
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
        coin_type = self.get_coin_type(row['マーケット'])
        if row['マーケット'].endswith('_jpy'):
            self.profit += (row['価格'] - self.acq_costs[coin_type]) * row['数量']
            self.jpy -= row['価格'] * row['数量']
        elif row['マーケット'].endswith('_btc'):
            # Call an API to get a fair value at this moment.
            fair_value = self.get_fair_value(row['日時'], 'BTC_JPY')
            new_coins = row['価格'] * row['数量']
            if self.coins['btc'] == 0:
                # If this was first time to have BTC
                self.coins['btc'] = new_coins
                self.acq_costs['btc'] = fair_value
            else:
                former_cost = self.acq_costs['btc'] * self.coins['btc']
                cost = former_cost + fair_value * new_coins
                self.coins['btc'] += new_coins
                self.acq_costs['btc'] = cost / self.coins['btc']
                print('price =', row['価格'] * fair_value, coin_type)
        self.coins[coin_type] -= row['数量']
    def ask(self, row):
        coin_type = self.get_coin_type(row['マーケット'])
        if row['マーケット'].endswith('_jpy'):
            jpy = row['数量'] * row['価格']
            self.jpy += jpy
            if self.coins[coin_type] == 0:
                # if this was the first time to by this type of coin
                self.coins[coin_type] = row['数量']
                self.acq_costs[coin_type] = row['価格']
            else:
                former_cost = self.acq_costs[coin_type] * self.coins[coin_type] 
                cost = former_cost + jpy
                self.coins[coin_type] += row['数量']
                self.acq_costs[coin_type] = cost / self.coins[coin_type]
        elif row['マーケット'].endswith('_btc'):
            self.coins['btc'] -= row['価格'] * row['数量']
            # fair value at this moment
            fair_value = self.get_fair_value(row['日時'], coin_type.upper() + '_JPY')
            if self.coins[coin_type] == 0:
                # if this was the first time to by this type of coin
                self.coins[coin_type] = row['数量']
                self.acq_costs[coin_type] = fair_value
            else:
                former_cost = self.acq_costs[coin_type] * self.coins[coin_type]
                cost = former_cost + fair_value * row['数量']
                self.coins[coin_type] += row['数量']
                self.acq_costs[coin_type] = cost / self.coins[coin_type]
    def calculate(self, num_of_tx = 0):
        n = 0
        for index, row in self.trade.iterrows():
            n = n + 1
            print(row)
            if row['取引種別'] == '売り':
                cal.bid(row)
            elif row['取引種別'] == '買い':
                cal.ask(row)
            cal.print_status()
            if n == num_of_tx:
                break

# purchase by credit card
cal = ProfitCalculator()
cal.read_log()
cal.purchase('mona', 233.01, 10000)
cal.calculate()

