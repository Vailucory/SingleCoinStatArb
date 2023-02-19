
import binance.error as ApiErrors
from binance.um_futures import UMFutures
from config_manager import ConfigManager
from DataBase import Repository
import time
from numpy import double

TICKER_PRICE_WEIGHT = 1
CANCEL_ORDER_WEIGHT = 1
GET_SYMBOL_ORDERS_WEIGHT = 1
GET_ORDERS_WEIGHT = 40
GET_TRADES_WEIGHT = 5
BALANCE_WEIGHT = 5
EXCHANGE_INFO_WEIGHT = 1
LEVERAGE_INFO_WEIGHT = 1
KLINES_1_100_WEIGHT = 1
KLINES_100_500_WEIGHT = 2
KLINES_500_1000_WEIGHT = 5
KLINES_1000_1500_WEIGHT = 10
CHANGE_LEVERAGE_WEIGHT = 1
MAX_WEIGHT_PER_MINUTE = 1200
MAX_ORDERS_PER_10SEC = 50
MAX_WEIGHT_PER_LOOP = 8


def static_init(cls):
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls

@static_init
class BinanceAPI:

    client = UMFutures(ConfigManager.config['api_key'], ConfigManager.config['api_secret'])

    @staticmethod
    def place_api_order(symbol:str, quantity:double, side:str, reduceOnly:bool=False, max_retries=40):
        i = 0
        while True:
            try:
                i += 1
                order = BinanceAPI.client.new_order(symbol=symbol,
                                        side=side,
                                        type='MARKET',
                                        reduceOnly=reduceOnly,
                                        quantity=quantity) 
                return order
            except ApiErrors.ServerError as e:
                if i >= max_retries:
                    Repository.log_message(f"{symbol}:{side}:{quantity} | {str(e)}")    
                    raise
                time.sleep(15)

    @staticmethod
    def cancel_order(symbol:str, orderId:int, max_retries=40) -> bool:
        i = 0
        while True:
            try:
                i += 1
                while True:
                    if is_api_call_possible(CANCEL_ORDER_WEIGHT):
                        t = time.time() * 1000
                        Repository.add_weight(weight=CANCEL_ORDER_WEIGHT, method='cancel_order', time=t)
                        BinanceAPI.client.cancel_order(symbol, orderId = orderId)
                        Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                        return True
                    #wait 1 second and try to repeat
                    time.sleep(1)
            except ApiErrors.ServerError as e:
                if i >= max_retries:
                    Repository.log_message(f"ERROR CANCELLING ORDER | {str(e)}")    
                    raise
                time.sleep(15)
            except ApiErrors.ClientError as e:
                #order allready closed
                if e.error_code == -2011:
                    return False
                raise 

    @staticmethod
    def place_api_market_stop_order(symbol:str, stop_price:double, side:str, max_retries=40):
        i = 0
        while True:
            try:
                i += 1
                order = BinanceAPI.client.new_order(symbol=symbol,
                                        side=side,
                                        timeInForce='GTC',
                                        type='STOP_MARKET',
                                        closePosition=True,
                                        stopPrice=stop_price) 
                return order
            except ApiErrors.ServerError as e:
                if i >= max_retries:
                    Repository.log_message(f"{symbol}:{side} | {str(e)}")    
                    raise
                time.sleep(15)

    @staticmethod
    def get_ticker_price(coin:str):
        while True:
            if is_api_call_possible(TICKER_PRICE_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=TICKER_PRICE_WEIGHT, method='ticker_price', time=t)
                price = double(BinanceAPI.client.ticker_price(coin)['price'])
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return price
            #wait 1 second and try to repeat
            time.sleep(1)

    @staticmethod
    def get_account_trades(symbol):
        while True:
            if is_api_call_possible(GET_TRADES_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=GET_TRADES_WEIGHT, method='get_account_trades', time=t)
                trades = BinanceAPI.client.get_account_trades(symbol)
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return trades
            #wait 1 second and try to repeat
            time.sleep(1)

    @staticmethod
    def get_all_account_orders(symbol):
        while True:
            if is_api_call_possible(GET_SYMBOL_ORDERS_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=GET_SYMBOL_ORDERS_WEIGHT, method='get_orders', time=t)
                orders = BinanceAPI.client.get_all_orders(symbol=symbol)
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return orders
            #wait 1 second and try to repeat
            time.sleep(1)

    @staticmethod
    def change_leverage(coin, leverage):
        while True:
            if is_api_call_possible(CHANGE_LEVERAGE_WEIGHT):
                t = time.time() * 1000    
                Repository.add_weight(weight=CHANGE_LEVERAGE_WEIGHT, method='change_leverage', time=t)
                max_notional = int(BinanceAPI.client.change_leverage(coin, leverage)['maxNotionalValue'])
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return max_notional
            #wait 1 second and try to repeat
            time.sleep(1)

    @staticmethod
    def get_production_balance():
        while True:
            if is_api_call_possible(BALANCE_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=BALANCE_WEIGHT, method='balance', time=t)
                balance = [{'balance':double(entry['balance']), 'available_balance':double(entry['availableBalance'])} for entry in BinanceAPI.client.balance() if entry['asset'] == 'USDT'][0]
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return balance
            #wait 1 second and try to repeat
            time.sleep(1)

    @staticmethod
    def seed_currencies():
        while True:
            if is_api_call_possible(LEVERAGE_INFO_WEIGHT+EXCHANGE_INFO_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=EXCHANGE_INFO_WEIGHT, method='exchange_info', time=t)
                prices = BinanceAPI.client.exchange_info()['symbols']
                currencies = []
                with open('cryptolist.txt', 'r') as file:
                    currencies = [c.strip() for c in file.readlines()]
 
                values =  [(p['symbol'],p['pricePrecision'], p['quantityPrecision'], p['filters'][5]['notional'], p['filters'][0]['tickSize']) for p in prices if currencies.__contains__(p['symbol'])]
                Repository.seed_currencies(values)
                Repository.add_weight(weight=LEVERAGE_INFO_WEIGHT, method='leverage_brackets', time=t)
                leverages = {entry['symbol']:20 if max([b['initialLeverage'] for b in entry['brackets']]) >= 20 else max([b['initialLeverage'] for b in entry['brackets']]) for entry in BinanceAPI.client.leverage_brackets() if currencies.__contains__(entry['symbol']) }
                leverages_to_update = [(leverages[currency], currency) for currency in currencies]
                Repository.update_leverages(leverages_to_update)

                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                break
            #wait 1 second and try to repeat
            time.sleep(1)

    @staticmethod
    def update_currencies():
        while True:
            if is_api_call_possible(LEVERAGE_INFO_WEIGHT+EXCHANGE_INFO_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=EXCHANGE_INFO_WEIGHT, method='exchange_info', time=t)
                prices = BinanceAPI.client.exchange_info()['symbols']
                currencies = [c[0] for c in Repository.get_all_currencies()]
                currencies_to_update = [(p['pricePrecision'], p['quantityPrecision'], p['filters'][5]['notional'], p['filters'][0]['tickSize'], p['symbol']) for p in prices if currencies.__contains__(p['symbol'])]
                Repository.update_currencies(currencies_to_update)

                Repository.add_weight(weight=LEVERAGE_INFO_WEIGHT, method='leverage_brackets', time=t)
                leverages = {entry['symbol']:20 if max([b['initialLeverage'] for b in entry['brackets']]) >= 20 else max([b['initialLeverage'] for b in entry['brackets']]) for entry in BinanceAPI.client.leverage_brackets() if currencies.__contains__(entry['symbol']) }
                leverages_to_update = [(leverages[currency], currency) for currency in currencies]
                Repository.update_leverages(leverages_to_update)

                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                break
            #wait 1 second and try to repeat
            time.sleep(1)

    @staticmethod
    def get_klines(kline_interval:str, bar_count:int, symbols):
        result={}
        for symbol in symbols:
            kline={}
            while True:
                if is_api_call_possible(KLINES_100_500_WEIGHT):
                    t = time.time() * 1000
                    Repository.add_weight(weight=KLINES_100_500_WEIGHT, method=f'klines_{bar_count}', time=t)
                    kline = {symbol:BinanceAPI.client.klines(symbol, kline_interval, limit=bar_count)}
                    Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                    break
                    #wait 1 second and try to repeat
                time.sleep(1)

            if len(kline[symbol]) == bar_count:
                result.update(kline)
            else:
                raise Exception('Not enough bars to extract: {0}/{1}'.format(len(kline[symbol]), bar_count))
        return result
    
    @staticmethod
    def get_kline(kline_interval:str, limit:int, symbol:str, start_time:int = None):

        api_request_weight = 0
        if limit <= 100:
            api_request_weight = KLINES_1_100_WEIGHT
        elif limit <= 500:
            api_request_weight = KLINES_100_500_WEIGHT
        elif limit <= 1000:
            api_request_weight = KLINES_500_1000_WEIGHT
        else:
            api_request_weight = KLINES_1000_1500_WEIGHT

        while True:
            if is_api_call_possible(KLINES_100_500_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=api_request_weight, method=f'klines_{limit}', time=t)
                kline = BinanceAPI.client.klines(symbol, kline_interval, limit=limit) if start_time is None else BinanceAPI.client.klines(symbol, kline_interval, limit=limit, startTime=start_time)
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return kline
                #wait 1 second and try to repeat
            time.sleep(1)


def is_api_call_possible(weight:int) -> bool:
    return (Repository.get_weight_for_last_minute(int(time.time()*1000)) + weight) < MAX_WEIGHT_PER_MINUTE