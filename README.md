# Calculation of profits in cryptcurrency trading
This script calculates profits(in Japanese Yen) in the course of trading of cryptcurrencies.

## Available Exchanges(only spot trading)
* [Zaif](https://zaif.jp?ac=grgdh633wu)
* [bitFlyer](https://bitflyer.jp/)
* [Bitbank](https://bitbank.cc/)

## Requirements
* Python 3.x
* pyenv
* virtualenv

If not installed, you can install by the following.
```bash
brew install pyenv
pyenv install 3.6.3
pyenv install virutalenv
```

## Setup
```bash
virtualenv env
source env/bin/activate
pip install -r requirments.txt
```

## Usage
Download CSV files for trading history and place them to the root directory.

Execute the following command.
```bash
python profits.py
```

## Reference
* [仮想通貨に関する所得の計算方法等について](https://www.nta.go.jp/shiraberu/zeiho-kaishaku/joho-zeikaishaku/shotoku/shinkoku/171127/01.pdf)
