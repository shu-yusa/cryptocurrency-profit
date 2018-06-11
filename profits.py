import traceback
import os
import math
import json
import pandas as pd
from datetime import timedelta
import requests


# global settings
wallets = {
    'ZAIF': 'zaif',
    'BF': 'bitflyer',
    'BITBANK': 'bitbank',
    'BLOCKCHAIN.INFO': 'blockchain.info',
    'TIPMONA': 'tipmona',
    'MONAPPY': 'monappy',
    'MONAWALLET': 'mona_wallet',
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
    'ICO': 'ICO',
}
csv_types = {
    'ZAIF_TRADE': 'zaif_trade',
    'ZAIF_CREDIT_TRADE': 'zaif_credit_trade',
    'ZAIF_DEPOSIT': 'zaif_deposit',
    'ZAIF_ERC20_DEPOSIT': 'zaif_erc20_deposit',
    'ZAIF_WITHDRAW': 'zaif_withdraw',
    'ZAIF_PURCHASE': 'zaif_purchase',
    'ZAIF_BONUS': 'zaif_bonus',
    'BCINFO_PURCHASE': 'bcinfo_purchase',
    'BITFLYER': 'bitflyer',
    'BITBANK': 'bitbank',
    'BITBANK_DEPOSIT_WITHDRAW': 'bitbank_deposit_withdraw',
    'MONAPPY': 'monappy',
    'MONAWALLET': 'monawallet',
    'TIPMONA': 'tipmona',
    'ICO': 'ico',
}


class TradeHistory:
    columns = [
        'market', 'type', 'price', 'cost', 'amount', 'time',
        'exchange', 'profit', 'total_profit',
    ]
    bf_currencies = [
        'BTC', 'ETH', 'ETC', 'LTC', 'BCH', 'MONA', 'JPY',
    ]

    def __init__(self):
        self.data = pd.DataFrame(columns=TradeHistory.columns)

    def __getattr__(self, name):
        # wrapping pandas dataframe
        return self.data[name]

    def head(self, rows=5):
        return self.data.head(rows)

    def tail(self, rows=5):
        return self.data.tail(rows)

    def set_data(self, data, type):
        self.data = self.format_data(data, type)

    def append_data(self, data, type, currency=''):
        self.data = self.data.append(self.format_data(data, type, currency))

    def format_data(self, data, type, currency=''):
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
                    'exchange': wallets['BF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['BITBANK']:
            def rule(row, currency):
                type_map = {
                    'sell': tx_types['BID'],
                    'buy': tx_types['ASK'],
                }

                def market_map(market):
                    if market == 'bcc_btc':
                        return 'bch_btc'
                    elif market == 'bcc_jpy':
                        return 'bch_jpy'
                    else:
                        return market
                return {
                    'time': row['取引日時'],
                    'type': type_map[row['売/買']],
                    'price': row['価格'],
                    'market': market_map(row['通貨ペア']),
                    'amount': row['数量'],
                    'cost': row['手数料'],
                    'exchange': wallets['BITBANK'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['BITBANK_DEPOSIT_WITHDRAW']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'type': row['種別'],
                    'amount': row['金額'],
                    'market': row['通貨'],
                    'price': 0,
                    'cost': row['手数料'],
                    'exchange': wallets['BITBANK'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ZAIF_TRADE']:
            type_map = {
                'bid': tx_types['ASK'],
                'ask': tx_types['BID'],
            }

            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'type': type_map[row['取引種別']],
                    'price': row['価格'],
                    'market': row['マーケット'],
                    'amount': row['数量'],
                    'cost': row['取引手数料'],
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ZAIF_DEPOSIT']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': currency,
                    'type': tx_types['DEPOSIT'],
                    'price': 0,
                    'cost': 0,
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ZAIF_ERC20_DEPOSIT']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': row['トークン'].lower(),
                    'type': tx_types['DEPOSIT'],
                    'price': 0,
                    'cost': 0,
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ZAIF_PURCHASE']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'amount': row['数量'],
                    'market': row['通貨'],
                    'type': tx_types['PURCHASE'],
                    'price': 0,
                    'cost': row['価格'],
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['BCINFO_PURCHASE']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'amount': row['数量'],
                    'market': row['通貨'],
                    'type': tx_types['PURCHASE'],
                    'price': 0,
                    'cost': row['価格'],
                    'exchange': wallets['BLOCKCHAIN.INFO'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ZAIF_BONUS']:
            def rule(row, currency):
                return {
                    'time': row['支払日時'],
                    'amount': row['支払ボーナス'],
                    'market': 'jpy',
                    'type': tx_types['RECEIVE'],
                    'price': 0,
                    'cost': 0,
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ZAIF_CREDIT_TRADE']:
            def rule(row, currency):
                return {
                    'time': row['決済完了日時'],
                    'amount': row['損益（円）'],
                    'market': 'jpy',
                    'type': tx_types['RECEIVE'],
                    'price': 0,
                    'cost': 0,
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ZAIF_WITHDRAW']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'amount': row['金額'],
                    'market': currency,
                    'type': tx_types['WITHDRAW'],
                    'price': 0,
                    'cost': row['手数料'],
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['MONAPPY']:
            type_map = {
                '受け取り': tx_types['RECEIVE'],
                '送金': tx_types['SEND'],
                '手数料': tx_types['FEE'],
            }

            def rule(row, currency):
                return {
                    'time': row['日付'],
                    'amount': abs(row['金額']),
                    'market': 'mona',
                    'type': type_map[row['種別']],
                    'price': 0,
                    'cost': 0,
                    'exchange': wallets['MONAPPY'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['TIPMONA']:
            def rule(row, currency):
                return {
                    'time': row['日付'],
                    'amount': abs(row['金額']),
                    'market': 'mona',
                    'type': row['種別'],
                    'price': 0,
                    'cost': row['手数料'],
                    'exchange': wallets['TIPMONA'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['MONAWALLET']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'type': row['種別'],
                    'amount': row['金額'],
                    'market': row['通貨'],
                    'price': 0,
                    'cost': row['手数料'],
                    'exchange': wallets['MONAWALLET'],
                    'profit': 0,
                    'total_profit': 0,
                }
        elif type == csv_types['ICO']:
            def rule(row, currency):
                return {
                    'time': row['日時'],
                    'amount': row['数量'],
                    'market': row['マーケット'],
                    'type': tx_types['ICO'],
                    'price': row['金額'],
                    'cost': 0,
                    'exchange': wallets['ZAIF'],
                    'profit': 0,
                    'total_profit': 0,
                }
        df = pd.DataFrame(
            [rule(row, currency) for index, row in data.iterrows()],
            columns=TradeHistory.columns
        )
        return df


class ProfitCalculator:
    # variation of coins
    coins = [
        'jpy', 'btc', 'bch', 'eth', 'mona', 'xem',
        'zaif', 'xcp', 'pepecash', 'cicc', 'erc20.cms', 'mosaic.cms',
        # 'ncxc', 'jpyz', 'bitcrystals', 'sjcx', 'fscc',
    ]
    # Timestamps for hard forks
    hf_timestamps = {
        'bch': {
            'src': 'btc',
            'timestamp': 1501611180,
        },
    }
    hf_wallets = [
        wallets['ZAIF'],
        wallets['BF'],
    ]
    # Endpoint of Zaif API
    zaif_api = 'https://zaif.jp/zaif_chart_api/v1/history'
    # Endpoint of Bitbank API
    bitbank_api = "https://public.bitbank.cc/{pair}/candlestick/1min/{time}"

    def __init__(self, initial={}):
        # Amount of every coin
        self.coins = {}
        for coin in ProfitCalculator.coins:
            self.coins[coin] = {w: 0 for w in wallets.values()}
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
        # cache of chart from Bitbank
        self.chart_cache = {
            "BTC_JPY": None,
            "BCC_JPY": None,
            "MONA_JPY": None,
        }

    def ceil(self, data):
        return data
        return math.ceil(data)

    def has_coin(self, coin):
        return round(coin, 8) == 0

    def print_status(self):
        for year in self.profit.keys():
            print('-- ', year, ' ---------------------------------')
            for key, row in self.coins.items():
                if sum(self.coins[key].values()) > 0:
                    print(key.upper() + ':', round(sum(self.coins[key].values()), 9))
                    for wallet, amount in self.coins[key].items():
                        if round(amount, 9) > 0:
                            print('   ', wallet, ':', round(amount, 9))
                    print(key.upper() + ' Acquisition cost:', round(self.acq_costs[key], 9))
            print()
            print('As of:', self.last_tx_time)
            print('Profits:', self.profit[year])
            print('Spent:', round(self.deposit_jpy))
            print(
                'Acquisition cost:',
                sum([sum(self.coins[c].values()) * self.acq_costs[c] for c in ProfitCalculator.coins])
            )
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

    def get_fair_value(self, time, symbol, exchange):
        time = time - timedelta(seconds=time.second)
        if exchange == wallets['ZAIF'] or symbol not in self.chart_cache.keys():
            params = {
                'symbol': symbol.upper(),
                'resolution': '1',
                'from': int(time.timestamp()),
                'to': int(time.timestamp()),
            }
            response = requests.get(ProfitCalculator.zaif_api, params=params)
            if response.status_code != 200:
                raise Exception('return status code is {}'.format(response.status_code))
            data = json.loads(response.json())
            count = data['data_count']
            if (count != 0):
                data = pd.DataFrame.from_dict(data['ohlc_data'])
            return data['close'][0]
        elif exchange == wallets['BITBANK']:
            ts = int(time.timestamp()) * 1000
            symbol = symbol.replace('BCH', 'BCC')
            if self.chart_cache[symbol] is None or ts not in self.chart_cache[symbol].index:
                time = time - timedelta(hours=9)
                url = self.bitbank_api.format(pair=symbol.lower(), time=time.strftime('%Y%m%d'))
                response = requests.get(url)
                data = response.json()
                if data['success'] == 0:
                    print('time=', time)
                    print('symbol=', symbol)
                    print('url=', url)
                    print(data)
                    raise Exception()
                ohlcv = data['data']['candlestick'][0]["ohlcv"]
                df = pd.DataFrame(ohlcv, columns=['open', 'high', 'low', 'close', 'volume', 'time'], dtype=float)
                df['time'] = df['time'].astype('int')
                self.chart_cache[symbol] = df.set_index('time')
            return self.chart_cache[symbol].loc[ts]['close']

    def bid(self, row):
        # Unit of fee in bid is jpy or btc
        coin_type = self.get_coin_type(row['market'])
        if row['market'].endswith('_jpy'):
            self.profit[row['time'].year] += (row['price'] - self.ceil(self.acq_costs[coin_type])) * row['amount']
            if row['exchange'] == wallets['ZAIF']:
                self.coins['jpy'][row['exchange']] += row['price'] * row['amount'] - row['cost']
            elif row['exchange'] == wallets['BF']:
                self.coins['jpy'][row['exchange']] += math.floor(row['price'] * row['amount']) - row['cost']
        elif row['market'].endswith('_btc'):
            # Call an API to get a fair value at this moment.
            btc_fair_value = self.get_fair_value(row['time'], 'BTC_JPY', row['exchange'])
            alt_fair_value = self.get_fair_value(row['time'], coin_type.upper() + '_JPY', row['exchange'])
            # profits arise from selling ALT coins
            self.profit[row['time'].year] += (alt_fair_value - self.ceil(self.acq_costs[coin_type])) * row['amount']

            # update acquisition cost of BTC
            new_coins = row['price'] * row['amount']
            total_btc = sum(self.coins['btc'].values())
            if self.has_coin(total_btc):
                # If this was first time to have BTC
                self.coins['btc'][row['exchange']] = new_coins - row['cost']
                self.acq_costs['btc'] = btc_fair_value
            else:
                former_cost = self.acq_costs['btc'] * total_btc
                cost = former_cost + btc_fair_value * new_coins
                self.coins['btc'][row['exchange']] += new_coins - row['cost']
                self.acq_costs['btc'] = cost / sum(self.coins['btc'].values())
        self.coins[coin_type][row['exchange']] -= row['amount']

    def ask(self, row):
        # Unit of fee in ask is buying currency
        coin_type = self.get_coin_type(row['market'])
        total_coins = sum(self.coins[coin_type].values())
        if row['market'].endswith('_jpy'):
            jpy = row['amount'] * row['price']
            if row['exchange'] == wallets['ZAIF']:
                self.coins['jpy'][row['exchange']] -= jpy
            elif row['exchange'] == wallets['BF']:
                self.coins['jpy'][row['exchange']] -= self.ceil(jpy)

            if self.has_coin(total_coins):
                # if this was the first time to by this type of coin
                self.coins[coin_type][row['exchange']] = row['amount'] - row['cost']
                self.acq_costs[coin_type] = row['price']
            else:
                former_cost = self.acq_costs[coin_type] * total_coins
                cost = former_cost + jpy
                self.coins[coin_type][row['exchange']] += row['amount'] - row['cost']
                self.acq_costs[coin_type] = cost / sum(self.coins[coin_type].values())
        elif row['market'].endswith('_btc'):
            self.coins['btc'][row['exchange']] -= row['price'] * row['amount']
            # fair value at this moment
            fair_value = self.get_fair_value(row['time'], coin_type.upper() + '_JPY', row['exchange'])
            if self.has_coin(total_coins):
                # if this was the first time to by this type of coin
                self.coins[coin_type][row['exchange']] = row['amount'] - row['cost']
                self.acq_costs[coin_type] = fair_value
            else:
                former_cost = self.acq_costs[coin_type] * total_coins
                cost = former_cost + fair_value * row['amount']
                self.coins[coin_type][row['exchange']] += row['amount'] - row['cost']
                self.acq_costs[coin_type] = cost / sum(self.coins[coin_type].values())

    def purchase(self, row):
        self.deposit_jpy += row['cost']
        coin_type = row['market']
        total_coins = sum(self.coins[coin_type].values())
        if self.has_coin(total_coins):
            # if not coin yet
            self.coins[coin_type][row['exchange']] = row['amount']
            self.acq_costs[coin_type] = row['cost'] / sum(self.coins[coin_type].values())
        else:
            # if you already have coins, take moving average.
            former_cost = self.acq_costs[coin_type] * total_coins
            new_cost = former_cost + row['cost']
            self.coins[coin_type][row['exchange']] += row['amount']
            self.acq_costs[coin_type] = new_cost / sum(self.coins[coin_type].values())

    def deposit(self, row):
        self.coins[row['market']][row['exchange']] += row['amount'] - row['cost']
        if row['market'] == 'jpy':
            self.deposit_jpy += row['amount'] - row['cost']

    def withdraw(self, row):
        self.coins[row['market']][row['exchange']] -= row['amount'] + row['cost']
        if row['market'] == 'jpy':
            self.deposit_jpy -= row['amount'] + row['cost']

    def send(self, row):
        self.withdraw(row)

    def receive(self, row):
        self.coins[row['market']][row['exchange']] += row['amount'] - row['cost']

    def ico(self, row):
        [target, source] = row['market'].split('_')
        self.coins[source][row['exchange']] -= row['price']
        # self.coins[target][row['exchange']] += row['amount']
        source_fair_value = self.get_fair_value(row['time'], source.upper() + '_JPY', row['exchange'])
        target_fair_value = source_fair_value * row['price'] / row['amount']
        self.acq_costs[target] = target_fair_value
        self.profit[row['time'].year] += (source_fair_value - self.ceil(self.acq_costs[source])) * row['price']

    def fee(self, row):
        self.coins[row['market']][row['exchange']] -= row['amount']

    def check_hard_fork(self, row):
        for coin, flag in self.hf_flags.items():
            if row['time'].timestamp() > ProfitCalculator.hf_timestamps[coin]['timestamp'] and not flag:
                for wallet, coins in self.coins[ProfitCalculator.hf_timestamps[coin]['src']].items():
                    if wallet in self.hf_wallets:
                        self.coins[coin][wallet] = coins
                self.hf_flags[coin] = True

    def calculate(self, num_of_tx=-1):
        inv_tx_types = {v: k for k, v in tx_types.items()}
        for index, row in self.trade.data.iterrows():
            prev_profit = self.profit[row['time'].year]
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
                    self.trade.data.loc[index, 'total_profit'] = self.profit[row['time'].year]
                    self.trade.data.loc[index, 'profit'] = self.profit[row['time'].year] - prev_profit
            self.print_status()
            if index == num_of_tx:
                break
        self.trade.data.to_csv('foo.csv')

    def output(self, path):
        pass


if __name__ == "__main__":
    # List of csv files for transaction history
    data_list = {
        csv_types['ZAIF_TRADE']: [
            {'path': '52150_2017_1.csv'},
            {'path': '52150_2018_1.csv'},
        ],
        csv_types['ZAIF_CREDIT_TRADE']: [
            {'path': '52150_2017_2.csv'},
        ],
        csv_types['ZAIF_DEPOSIT']: [
            {'path': 'jpy_deposit.csv', 'currency': 'jpy'},
            {'path': 'btc_deposit.csv', 'currency': 'btc'},
            {'path': 'mona_deposit.csv', 'currency': 'mona'},
        ],
        csv_types['ZAIF_WITHDRAW']: [
            {'path': 'btc_withdraw.csv', 'currency': 'btc'},
            {'path': 'bch_withdraw.csv', 'currency': 'bch'},
            {'path': 'eth_withdraw.csv', 'currency': 'eth'},
            {'path': 'mona_withdraw.csv', 'currency': 'mona'},
        ],
        csv_types['ZAIF_ERC20_DEPOSIT']: [
            {'path': 'erc20_deposit.csv', 'currency': 'erc20.cms'},
        ],
        csv_types['ZAIF_BONUS']: [
            {'path': 'obtain_bonus.csv'},
        ],
        csv_types['BITFLYER']: [
            {'path': 'TradeHistory.csv'},
        ],
        csv_types['BITBANK']: [
            {'path': 'trade_history_bitbank.csv'},
        ],
        csv_types['MONAPPY']: [
            {'path': 'monappy_transaction.csv'},
        ],
        # custom csv files
        csv_types['BITBANK_DEPOSIT_WITHDRAW']: [
            {'path': 'deposit_withdraw_bitbank.csv'},
        ],
        csv_types['ZAIF_PURCHASE']: [
            {'path': 'purchase.csv'},
        ],
        csv_types['BCINFO_PURCHASE']: [
            {'path': 'purchase_bcinfo.csv'},
        ],
        csv_types['MONAWALLET']: [
            {'path': 'mona_wallet.csv'},
        ],
        csv_types['TIPMONA']: [
            {'path': 'tipmona.csv'},
        ],
        csv_types['ICO']: [
            {'path': 'ico.csv'}
        ],
    }

    cal = ProfitCalculator()
    cal.load_history(data_list)
    cal.calculate()
    cal.print_status()
