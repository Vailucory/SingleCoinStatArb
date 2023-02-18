from binance.um_futures import UMFutures
from config_manager import ConfigManager
from numpy import double
from DataBase import Repository, orders_definition
import time

MAX_WEIGHT_PER_MINUTE = 1200

def is_api_call_possible(weight:int) -> bool:
    return (Repository.get_weight_for_last_minute(int(time.time()*1000)) + weight) < MAX_WEIGHT_PER_MINUTE

def check_balance():
    config = ConfigManager.config
    client = UMFutures(config['api_key'], config['api_secret'])
    while True:
            if is_api_call_possible(5):
                t = time.time() * 1000
                Repository.add_weight(weight=5, method='balance', time=t)
                balance_entry = [e for e in client.balance() if e['asset'] == 'USDT'][0]
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                commission = sum([double(order[orders_definition.price])*double(order[orders_definition.origQty])*0.0004 for order in Repository.get_all_market_orders()])
                profit = double(balance_entry['balance']) - commission + double(balance_entry['crossUnPnl'])
                result = {}
                result.update({'asset':balance_entry['asset']})
                result.update({'balance':balance_entry['balance']})
                result.update({'crossUnPnl':balance_entry['crossUnPnl']})
                result.update({'availableBalance':balance_entry['availableBalance']})
                result.update({'currentPureBalance':profit})
                return result
            #wait 1 second and try to repeat
            time.sleep(1)
    

if __name__ == '__main__':


    balance = check_balance()

    print(f"Asset: {balance['asset']}")
    print(f"Balance: {balance['balance']}")
    print(f"Unrealized Pnl: {balance['crossUnPnl']}")
    print(f"Available balance: {balance['availableBalance']}")
    print(f"Current balance after closing all orders: {balance['currentPureBalance']}")