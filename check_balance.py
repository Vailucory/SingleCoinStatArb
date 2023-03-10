from binance.um_futures import UMFutures
from config_manager import ConfigManager
from numpy import double
from DataBase import Repository, orders_definition
from Binance import BinanceAPI
import time

MAX_WEIGHT_PER_MINUTE = 1200

def is_api_call_possible(weight:int) -> bool:
    return (Repository.get_weight_for_last_minute(int(time.time()*1000)) + weight) < MAX_WEIGHT_PER_MINUTE

def check_balance():
    balance_entity = BinanceAPI.get_production_balance()
    commission = sum([double(order[orders_definition.price])*
                      double(order[orders_definition.origQty])*
                      0.0004 
                      for order in Repository.get_all_market_orders()])
    profit = double(balance_entity['balance']) - commission + double(balance_entity['crossUnPnl'])
    
    result = {}
    result.update({'asset':balance_entity['asset']})
    result.update({'balance':balance_entity['balance']})
    result.update({'crossUnPnl':balance_entity['crossUnPnl']})
    result.update({'availableBalance':balance_entity['available_balance']})
    result.update({'currentPureBalance':profit})
    return result

    

if __name__ == '__main__':


    balance = check_balance()

    print(f"Asset: {balance['asset']}")
    print(f"Balance: {balance['balance']}")
    print(f"Unrealized Pnl: {balance['crossUnPnl']}")
    print(f"Available balance: {balance['availableBalance']}")
    print(f"Current balance after closing all orders: {balance['currentPureBalance']}")