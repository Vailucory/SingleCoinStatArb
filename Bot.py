import datetime as datetime
from config_manager import ConfigManager
import time
from datetime import datetime
from DataBase import Repository, BacktestRepository, orders_definition 
from numpy import double
from multiprocessing import Process
from math_helper import MathHelper
import random
from Binance import BinanceAPI
from interval_converter import IntervalConverter

MAX_ORDERS_PER_10SEC = 50
BACKTEST_MODE = True
PRODUCTION_MODE = False
PUMP_DUMP_INTERCEPT_MODE = True
ORDER_CREATION_AVAILABLE_BALANCE_TRESHOLD = 30/100# 15%


def close_market_order(market, current_price, exit_time, type, limit_order=None, status=None):
    
    if PRODUCTION_MODE:                              
        leverage = market[orders_definition.leverage]
        coin_current_leverage, coin_current_max_notional = Repository.get_current_leverage_and_max_notional(market[orders_definition.symbol])
        if coin_current_leverage != leverage:
            coin_max_notional = BinanceAPI.change_leverage(market[orders_definition.symbol], leverage)
            Repository.update_current_leverage_and_max_notional(market[orders_definition.symbol], leverage, coin_max_notional)        
        else:
            coin_max_notional = coin_current_max_notional 
            leverage = coin_current_leverage
        
        side = 'SELL' if market[orders_definition.side] == 'BUY' else 'BUY'
        if status == 'NEW':
            if limit_order is None or BinanceAPI.cancel_order(market[orders_definition.symbol], limit_order['orderId']):
                order_market = BinanceAPI.place_api_order(symbol = market[orders_definition.symbol],
                                                          side = side,
                                                          reduceOnly=True,
                                                          quantity = market[orders_definition.origQty])
        elif status == 'FILLED':
            #already closed position with stop market so nothing needs to be done
            pass

        trades = BinanceAPI.get_account_trades(market[orders_definition.symbol])

        if len(trades) < 2:
            raise Exception(f'Less than 2 trades avalible for {market[orders_definition.symbol]}')
        
        first_enter_trade = [t for t in trades if t['orderId'] == market[orders_definition.orderId]][0]
        current_trades = [t for t in trades if t['time'] >= first_enter_trade['time']]

        enter_trades = [t for t in current_trades if t['side'] == first_enter_trade['side']]
        exit_trades = [t for t in current_trades if t['side'] != first_enter_trade['side']]
        last_exit_trade = exit_trades[-1]

        pnl = sum([double(t['realizedPnl']) for t in exit_trades])
        qty = market[orders_definition.origQty]
        exit_commission = sum([double(t['commission']) for t in exit_trades])
        enter_commission = sum([double(t['commission']) for t in enter_trades])

        exit_price = sum([double(t['quoteQty']) for t in exit_trades]) / qty
        enter_price = sum([double(t['quoteQty']) for t in enter_trades]) / qty

        trade = {}
        trade.update({'symbol':market[orders_definition.symbol]})      # first_enter                     
        trade.update({'openOrderId':first_enter_trade['orderId']})     # first_enter                        
        trade.update({'closeOrderId':last_exit_trade['orderId']})      # last_exit                             
        trade.update({'triggeredOrderType':type})                      # first_enter                 
        trade.update({'side':first_enter_trade['side']})               # first_enter      
        trade.update({'enterPrice':enter_price})                       # avg                         
        trade.update({'exitPrice':exit_price})                         # avg pos = (5 * 10 + 10 * 20) / 15 = 16,66                     
        trade.update({'qty':qty})                                      # +            
        trade.update({'realizedPnl':pnl})                              # +                             
        trade.update({'enterCommission':enter_commission})             # +            
        trade.update({'exitCommission':exit_commission})               # +              
        trade.update({'enterTime':first_enter_trade['time']})          # first_enter                
        trade.update({'exitTime':last_exit_trade['time']})             # last_exit                
        trade.update({'tradeId':market[orders_definition.tradeId]})  
        Repository.add_trade(trade)
        Repository.remove_orders(market[orders_definition.symbol])
        Repository.archive_order(market[:orders_definition.lastPrice]) # trim 2 unused field                         
        balance_entity = BinanceAPI.get_production_balance()
        Repository.set_available_balance(balance_entity['available_balance'])
        Repository.set_balance(balance_entity['balance'])
    elif BACKTEST_MODE:
        profit = market[orders_definition.currentProfit] - MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
        BacktestRepository.add_backtest_result(symbol=market[orders_definition.symbol],
                                    triggered_order_type=type,
                                    profit=profit-MathHelper.calculate_entering_market_commission(enter_price=market[orders_definition.price], quantity=market[orders_definition.origQty]),
                                    enter_price=market[orders_definition.price],
                                    exit_price=current_price,
                                    quantity=market[orders_definition.origQty],
                                    enter_time=market[orders_definition.updateTime],
                                    exit_time=exit_time,
                                    tradeId=market[orders_definition.tradeId])
        BacktestRepository.remove_orders(market[orders_definition.symbol])
        #we need to discard last two fields
        BacktestRepository.archive_order(market[:orders_definition.lastPrice])

        exit_commission = -MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
        BacktestRepository.set_available_balance(BacktestRepository.get_available_balance()+market[orders_definition.origQty]/market[orders_definition.leverage]*market[orders_definition.price]+exit_commission, exit_time)
        BacktestRepository.add_to_balance(profit, exit_time)   
        
def fill_backtest_order_with_defaults(order:dict):
    order.update({'symbol':None})
    order.update({'orderId':None})
    order.update({'status':None})
    order.update({'clientOrderId':None})
    order.update({'price':None})
    order.update({'avgPrice':0})
    order.update({'origQty':None})
    order.update({'executedQty':None})
    order.update({'cumQuote':None})
    order.update({'timeInForce':None})
    order.update({'type':None})
    order.update({'reduceOnly':None})
    order.update({'closePosition':None})
    order.update({'side':None})
    order.update({'positionSide':None})
    order.update({'stopPrice':None})
    order.update({'workingType':None})
    order.update({'priceProtect':None})
    order.update({'origType':None})
    order.update({'updateTime':None})

def manage_market_order_creation(coin, quantity, side, current_price, stop_price, leverage = 20, backtest_time = None):
    
    order_id = int(time.time() * 1000)
    
    try:
        if BACKTEST_MODE:
            BacktestRepository.set_available_balance(MathHelper.calculate_balance_after_entering_market(BacktestRepository.get_available_balance(), current_price, quantity, leverage), backtest_time)
            BacktestRepository.set_balance(BacktestRepository.get_balance()-MathHelper.calculate_entering_market_commission(current_price, quantity), backtest_time)
       
        market_order = {}
        stop_order = {}
    
        if PRODUCTION_MODE:
            market_order = BinanceAPI.place_api_order(symbol=coin,
                                                      side=side,
                                                      quantity=quantity)
            stop_market_side = 'BUY' if side == 'SELL' else 'SELL'
            stop_order = BinanceAPI.place_api_market_stop_order(symbol=coin, stop_price=stop_price, side=stop_market_side)                                    
        elif BACKTEST_MODE:
            fill_backtest_order_with_defaults(market_order)

        if BACKTEST_MODE:
            market_order['symbol'] = coin
            market_order['orderId'] = random.randint(1, 9999999)
            market_order['status'] = 'NOT EXIST'
            market_order['clientOrderId'] = 0
            market_order['price'] = current_price
            market_order['avgPrice'] = current_price
            market_order['origQty'] = quantity
            market_order['executedQty'] = 0
            market_order['cumQuote'] = 0
            market_order['timeInForce'] = 'GTC'
            market_order['type'] = 'MARKET'
            market_order['reduceOnly'] = '0'
            market_order['closePosition'] = '0'
            market_order['side'] = side
            market_order['positionSide'] = 'BOTH'
            market_order['stopPrice'] = 0
            market_order['workingType'] = 'CONTRACT_PRICE'
            market_order['priceProtect'] = 0
            market_order['origType'] = 'MARKET'
            market_order['updateTime'] = int(time.time()*1000)
        
        market_order.update({'tradeId':order_id})
        market_order.update({'leverage':leverage})
        stop_order.update({'tradeId':order_id})
        stop_order.update({'leverage':leverage})

        if BACKTEST_MODE:
            market_order.update({'lastPrice':current_price})
            market_order.update({'currentProfit':0})

        if BACKTEST_MODE:
            market_order['updateTime'] = backtest_time
            BacktestRepository.add_order(market_order)
        else:
            market_order['price'] = current_price
            Repository.add_order(market_order)
            Repository.add_order(stop_order)

    except Exception as err: 
        if BACKTEST_MODE:
            BacktestRepository.log_message(coin + str(err))    
        else:
            Repository.log_message(coin + str(err))    

def trade_calculator():
    while True:
        try:
            start_time = time.time()
            is_program_shutdown_started = Repository.get_is_program_shutdown_started()
            for coin in Repository.get_coins_with_open_orders():

                market_order = Repository.get_active_order_by_type(coin, 'MARKET') 
                stop_market_order = Repository.get_active_order_by_type(coin, 'STOP_MARKET')
                deviation, lin_reg_coef_a, lin_reg_coef_b = Repository.get_symbol_info(market_order[orders_definition.symbol])
                
                close_time = int(time.time()*1000)
                coin_current_price = BinanceAPI.get_ticker_price(coin)
                coefficients_linear_regression = (lin_reg_coef_a, lin_reg_coef_b)
                linear_regression_bound = MathHelper.calculate_polynom(close_time, coefficients_linear_regression)  
                price_stop_percentage = Repository.get_price_stop_percentage()
                market_exceed_limits = MathHelper.is_price_exceeded_limit(market_order[orders_definition.price], coin_current_price, market_order[orders_definition.leverage], price_stop_percentage, market_order[orders_definition.side])
                
                use_limit_stops = stop_market_order is not None 
                coin_limit_order = None
                if use_limit_stops:
                    coin_limit_order = [o for o in BinanceAPI.get_all_account_orders(symbol=coin)if o['clientOrderId'] == stop_market_order[orders_definition.clientOrderId]][0]

                if close_time - market_order[orders_definition.updateTime] > IntervalConverter.interval_to_time(ConfigManager.config['order_stop_limit_time']) * 1000:
                    #close trade via market and exit 
                    close_market_order(market_order, coin_current_price, close_time, 'TIME_STOP', coin_limit_order, 'NEW')
                    continue
                if use_limit_stops and coin_limit_order['executedQty'] != '0':
                    print(f'STOP MARKET TRIGGERED FOR {coin}')
                    coin_status = coin_limit_order['status']
                    close_market_order(market_order, coin_current_price, close_time, 'PRICE_STOP_LIMIT_EXCEEDED', coin_limit_order, coin_status)
                    Repository.log_message(f"{market_order[orders_definition.symbol]} exceed price stop limit with {market_order[orders_definition.symbol]}: {market_exceed_limits[1]}. TradeId: {market_order[orders_definition.tradeId]}")
                elif is_program_shutdown_started:
                    close_market_order(market_order, coin_current_price, close_time, 'PROGRAM_CLOSURE', coin_limit_order, 'NEW')                
                elif not use_limit_stops and market_exceed_limits[0]:
                    print(f'PRICE STOP LIMIT EXCEEDED FOR {coin}')
                    close_market_order(market_order, coin_current_price, close_time, 'PRICE_STOP_LIMIT_EXCEEDED', coin_limit_order, 'NEW')                
                    Repository.log_message(f"{market_order[orders_definition.symbol]} exceed price stop limit with {market_order[orders_definition.symbol]}: {market_exceed_limits[1]}. TradeId: {market_order[orders_definition.tradeId]}")
                #up: short(sell)
                #down: long(buy)
                #we entered when crossed upper deviation bound
                elif market_order[orders_definition.side] == 'SELL':
                    #close market order if current price is lower than linear regression bound
                    if coin_current_price <= linear_regression_bound:
                        close_market_order(market_order, coin_current_price, close_time, 'LINEAR_REGRESSION_CROSSING', coin_limit_order, 'NEW')
                #we entered when crossed lower deviation bound
                else:
                    #close market order if current price is higher than linear regression bound
                    if coin_current_price >= linear_regression_bound:
                        close_market_order(market_order, coin_current_price, close_time, 'LINEAR_REGRESSION_CROSSING', coin_limit_order, 'NEW')
                
        except Exception as err: 
            Repository.log_message(str(err))    
            print(str(err))

        end_time = time.time()
        delta_time = end_time-start_time
        print(f"{datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S]')} UTC | DONE CALCULATING ORDERS PROFIT | Time Elapsed: {int(delta_time)}")
        time.sleep(max(MathHelper.ceil(60 - delta_time), 1))


def process_symbols():
    #Repository.set_pairs_default_is_outside_deviation()
    if PRODUCTION_MODE:
        balance_entity = BinanceAPI.get_production_balance()
        Repository.set_available_balance(balance_entity['available_balance'])
        Repository.set_balance(balance_entity['balance'])
    while True:
        start_time = time.time()
        active_symbols = Repository.get_active_symbols() 
        
        for symbol_info in active_symbols:
            #exit if symbol information outdated
            d_time = int(time.time()*1000) - symbol_info[7]
            if IntervalConverter.interval_to_time(ConfigManager.config['kline_interval'])*1000 < d_time:
                continue  

            coin = symbol_info[0]
            is_outside_deviations = bool(symbol_info[4]) 

            #exit if active orders on coins
            if Repository.check_if_orders_available(coin):
                continue
                
            if is_outside_deviations:
                if not Repository.get_is_order_creation_allowed():
                    continue
                #getting linear regression polynom coefficients and deviation value
                deviation_multiplier = ConfigManager.config['lin_reg_deviation']
                deviation = symbol_info[1]
                coefficients_linear_regression = (symbol_info[2], symbol_info[3])
                epoch_time = int(time.time() * 1000)
                linear_regression_bound = MathHelper.calculate_polynom(epoch_time, coefficients_linear_regression)
                coin_price = BinanceAPI.get_ticker_price(coin)

                last_price = Repository.get_symbol_last_price(coin)
                deviation_coefficient = deviation_multiplier * 0.2
                return_deviation_coefficient = deviation_multiplier * 0.5
                if coin_price > linear_regression_bound and last_price - coin_price >= deviation * deviation_coefficient and coin_price - linear_regression_bound >= deviation * return_deviation_coefficient:
                    balance_entity = BinanceAPI.get_production_balance()
                    balance = balance_entity['balance'] 
                    if balance_entity['available_balance'] < balance * ORDER_CREATION_AVAILABLE_BALANCE_TRESHOLD:
                        continue

                    currency_info = Repository.get_currency(coin)
                    coin_max_notional = 1000000

                    leverage = currency_info[5]
                    coin_current_leverage, coin_current_max_notional = Repository.get_current_leverage_and_max_notional(coin)
                    if coin_current_leverage != leverage:
                        coin_max_notional = BinanceAPI.change_leverage(coin, leverage)
                        Repository.update_current_leverage_and_max_notional(coin, leverage, coin_max_notional)
                    else:
                        coin_max_notional = coin_current_max_notional 
                        leverage = coin_current_leverage
                    
                    stop_percentage = Repository.get_price_stop_percentage()
                    side = 'SELL'
                    quantity = MathHelper.calculate_quantity(total=balance,
                                                            entry_price=coin_price,
                                                            precision=currency_info[2],
                                                            minimum_notion=currency_info[3],
                                                            maximum_notion=coin_max_notional,
                                                            leverage=leverage)
                    stop_price = MathHelper.calculate_order_stop_price(current_price=coin_price, 
                                                                        tick_size=currency_info[4],
                                                                        stop_percentage=stop_percentage,
                                                                        leverage=leverage,
                                                                        side=side)
                    manage_market_order_creation(coin=coin, 
                                                quantity=quantity, 
                                                current_price=coin_price, 
                                                side=side,
                                                stop_price=stop_price,
                                                leverage=leverage) 
                    Repository.update_symbol_is_outside_deviation(symbol=coin, is_outside_deviation=0, time=epoch_time)   
                elif coin_price < linear_regression_bound and coin_price - last_price >= deviation * deviation_coefficient and linear_regression_bound - coin_price >= deviation * return_deviation_coefficient:
                    balance_entity = BinanceAPI.get_production_balance()
                    balance = balance_entity['balance'] 
                    if balance_entity['available_balance'] < balance * ORDER_CREATION_AVAILABLE_BALANCE_TRESHOLD:
                        continue

                    currency_info = Repository.get_currency(coin)
                    coin_max_notional = 1000000

                    leverage = currency_info[5]
                    coin_current_leverage, coin_current_max_notional = Repository.get_current_leverage_and_max_notional(coin)
                    if coin_current_leverage != leverage:
                        coin_max_notional = BinanceAPI.change_leverage(coin, leverage)
                        Repository.update_current_leverage_and_max_notional(coin, leverage, coin_max_notional)
                    else:
                        coin_max_notional = coin_current_max_notional 
                        leverage = coin_current_leverage
                    
                    stop_percentage = Repository.get_price_stop_percentage()
                    side = 'BUY'
                    quantity = MathHelper.calculate_quantity(total=balance,
                                                            entry_price=coin_price,
                                                            precision=currency_info[2],
                                                            minimum_notion=currency_info[3],
                                                            maximum_notion=coin_max_notional,
                                                            leverage=leverage)
                    stop_price = MathHelper.calculate_order_stop_price(current_price=coin_price, 
                                                                        tick_size=currency_info[4],
                                                                        stop_percentage=stop_percentage,
                                                                        leverage=leverage,
                                                                        side=side)
                    manage_market_order_creation(coin=coin, 
                                                quantity=quantity, 
                                                current_price=coin_price, 
                                                side=side,
                                                stop_price=stop_price,
                                                leverage=leverage) 
                    Repository.update_symbol_is_outside_deviation(symbol=coin, is_outside_deviation=0, time=epoch_time)       
                if epoch_time - Repository.get_symbol_iod_last_updated(coin) <= 3600000:
                    Repository.update_symbol_is_outside_deviation(symbol=coin, is_outside_deviation=0, time=epoch_time)    
                if (coin_price > linear_regression_bound and coin_price - linear_regression_bound < deviation * deviation_coefficient) or (coin_price < linear_regression_bound and linear_regression_bound - coin_price < deviation * deviation_coefficient):    
                    Repository.update_symbol_is_outside_deviation(symbol=coin, is_outside_deviation=0, time=epoch_time)    
                if PRODUCTION_MODE:
                    balance_entity = BinanceAPI.get_production_balance()
                    Repository.set_available_balance(balance_entity['available_balance'])
                    Repository.set_balance(balance_entity['balance'])
                #print('Exit due to orders placement|Time elapsed: {0}'.format(time.time() - pair_start_time)) 
                continue
                pass
            
            
            deviation_multiplier = ConfigManager.config['lin_reg_deviation']
            deviation = symbol_info[1]
            coefficients_up = (symbol_info[2], symbol_info[3] + (deviation * deviation_multiplier))
            coefficients_down = (symbol_info[2], symbol_info[3] - (deviation * deviation_multiplier))
            epoch_time = int(time.time() * 1000)
            upper_bound = MathHelper.calculate_polynom(epoch_time, coefficients_up)
            lower_bound = MathHelper.calculate_polynom(epoch_time, coefficients_down)

            coin_price = BinanceAPI.get_ticker_price(coin)

            if coin_price > upper_bound or coin_price < lower_bound:
                Repository.update_symbol_is_outside_deviation(symbol=coin, is_outside_deviation=1, time=epoch_time)     
                Repository.update_symbol_last_price(coin, coin_price)  
        end_time = time.time()
        delta_time = end_time-start_time
        print(f"{datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S]')} UTC | ALL PAIRS DONE | Time elapsed: {int(delta_time)}")
        time.sleep(max(MathHelper.ceil(60 - delta_time), 1)) 
     
def update_symbols():
    config = ConfigManager.config

    last_symbol_information_update = Repository.get_last_symbol_information_update()
    if last_symbol_information_update is not None: 
        d_time = int(time.time()) - last_symbol_information_update[2]
        if IntervalConverter.interval_to_time(last_symbol_information_update[0]) > d_time:
            print('Need to wait {0} seconds before next symbol information update'.format(IntervalConverter.interval_to_time(last_symbol_information_update[0])-d_time))
            time.sleep(IntervalConverter.interval_to_time(last_symbol_information_update[0])-d_time)
            pass
    while True:
        try: 
            t0 = time.time()   

            currencies=[c[0] for c in Repository.get_all_currencies()]
            klines_dictionary = BinanceAPI.get_klines(config['kline_interval'], config['bar_count'], currencies)
            
            #upsert symbols information
            symbols_to_upsert = []
            for coin, klines in klines_dictionary.items():

                closes = [double(k[4]) for k in klines]
                close_times = [t[6] for t in klines]

                deviation = MathHelper.calculate_deviation(closes)
                coefficients = MathHelper.calculate_linear_regression_coefficients(close_times, closes)
                symbols_to_upsert.append((deviation, coefficients[0], coefficients[1], int(time.time()*1000), coin))
            Repository.upsert_symbols(symbols_to_upsert)        
            t_end = time.time()
            delta_time = int(t_end-t0)
            Repository.add_symbol_information_update(bars=config['bar_count'],
                                        interval=config['kline_interval'],
                                        time_elapsed=delta_time,
                                        starting_time=int(t0))
            time_to_wait = IntervalConverter.interval_to_time(config['kline_interval']) - delta_time

            print('Done updating symbols')

            BinanceAPI.update_currencies()
            Repository.delete_old_weights()
            time.sleep(time_to_wait)

        except Exception as err:
            print(str(err))
            Repository.log_message(str(err))            


if __name__ == '__main__':

    p1 = Process(target=update_symbols)
    p2 = Process(target=process_symbols)
    p3 = Process(target=trade_calculator)
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()









