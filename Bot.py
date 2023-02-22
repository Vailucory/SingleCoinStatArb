from binance.um_futures import UMFutures
import datetime as datetime
from config_manager import ConfigManager
import time
from datetime import datetime
from DataBase import Repository, BacktestRepository, orders_definition 
from numpy import double, unique
from statsmodels.tsa.stattools import adfuller
from multiprocessing import Process
import threading
from itertools import combinations
from math_helper import MathHelper
import random
import binance.error as ApiErrors
from hurst import compute_Hc
from Binance import BinanceAPI, MAX_WEIGHT_PER_MINUTE
from interval_converter import IntervalConverter

MAX_ORDERS_PER_10SEC = 50
MAX_WEIGHT_PER_LOOP = 8
LOOPS_PER_MINUTE = int(MAX_WEIGHT_PER_MINUTE/MAX_WEIGHT_PER_LOOP) 
ONLINE_TEST_MODE = False
BACKTEST_MODE = True
PRODUCTION_MODE = False
PUMP_DUMP_INTERCEPT_MODE = True
ORDER_CREATION_AVAILABLE_BALANCE_TRESHOLD = 15/100# 15%


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
        if status == 'NORMAL':
            print('NORMAL')
            if limit_order is None or BinanceAPI.cancel_order(market[orders_definition.symbol], limit_order['orderId']):
                order_market = BinanceAPI.place_api_order(symbol = market[orders_definition.symbol],
                                                          side = side,
                                                          reduceOnly=True,
                                                          quantity = market[orders_definition.origQty])
        elif status == 'FILLED':
            #already closed position with stop market so don't need to do anything
            print('FILLED')

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
        trade.update({'hedgeId':market[orders_definition.hedgeId]})  
        Repository.add_trade(trade)
        Repository.remove_orders(market[orders_definition.symbol])
        Repository.archive_order(market)                         
        balance_entry = BinanceAPI.get_production_balance()
        Repository.set_available_balance(balance_entry['available_balance'])
        Repository.set_balance(balance_entry['balance'])
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
    elif ONLINE_TEST_MODE:
        profit = market[orders_definition.currentProfit] - MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
        Repository.add_test_result(symbol=market[orders_definition.symbol],
                                    triggered_order_type=type,
                                    profit=profit-MathHelper.calculate_entering_market_commission(enter_price=market[orders_definition.price], quantity=market[orders_definition.origQty]),
                                    enter_price=market[orders_definition.price],
                                    exit_price=current_price,
                                    enter_time=market[orders_definition.updateTime],
                                    exit_time=exit_time,
                                    tradeId=market[orders_definition.tradeId],
                                    leverage=market[orders_definition.leverage])
        Repository.remove_orders(market[orders_definition.symbol])
        #we need to discard 2 last fields
        Repository.archive_order(market[:orders_definition.lastPrice])                            
        Repository.set_available_balance(Repository.get_available_balance()+market[orders_definition.origQty]/market[orders_definition.leverage]*market[orders_definition.price]+profit)
        exit_commission = -MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
        Repository.add_to_balance(exit_commission)
        
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

        if BACKTEST_MODE or ONLINE_TEST_MODE:
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

        if BACKTEST_MODE or ONLINE_TEST_MODE:
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
    config = ConfigManager.config
    while True:
        try:
            start_time = time.time()
            is_program_shutdown_started = Repository.get_is_program_shutdown_started()
            if ONLINE_TEST_MODE:
                for order in Repository.get_all_market_orders():
                    current_price = BinanceAPI.get_ticker_price(order[orders_definition.symbol])
                    
                    
                    current_profit = order[orders_definition.currentProfit] + MathHelper.calculate_order_profit(enter_price=order[orders_definition.lastPrice],
                                                                                                        triggered_price=current_price,
                                                                                                        quantity=order[orders_definition.origQty],
                                                                                                        order_type=order[orders_definition.side])
                    Repository.add_to_balance(current_profit-order[orders_definition.currentProfit])
                    Repository.update_order_current_parameters(current_profit=current_profit,
                                                                last_price=current_price,
                                                                symbol=order[orders_definition.symbol])
            for coin1, coin2 in Repository.get_coins_with_open_orders_by_hedges():
                
                

                market1 = Repository.get_active_order_by_type(coin1, 'MARKET') 
                market2 = Repository.get_active_order_by_type(coin2, 'MARKET') 
                stop_limit1 = Repository.get_active_order_by_type(coin1, 'STOP_MARKET')
                stop_limit2 = Repository.get_active_order_by_type(coin2, 'STOP_MARKET')
                


                pair, lin_reg_coef_a, lin_reg_coef_b = Repository.get_pair_by_coins(market1[orders_definition.symbol], market2[orders_definition.symbol])
                close_time = int(time.time()*1000)

                coin1_current_price = BinanceAPI.get_ticker_price(coin1)
                coin2_current_price = BinanceAPI.get_ticker_price(coin2)
                
                
                   
                current_relation_price = coin1_current_price/coin2_current_price
                coefficients_linear_regression = (lin_reg_coef_a, lin_reg_coef_b)
                linear_regression_bound = MathHelper.calculate_polynom(close_time, coefficients_linear_regression)  
                price_stop_percentage = Repository.get_price_stop_percentage()
                market_exceed_limits = MathHelper.is_price_exceeded_limit(market1[orders_definition.price], coin1_current_price, market1[orders_definition.leverage], price_stop_percentage, market1[orders_definition.side])
                market2_exceed_limits = MathHelper.is_price_exceeded_limit(market2[orders_definition.price], coin2_current_price, market2[orders_definition.leverage], price_stop_percentage, market2[orders_definition.side])
                use_limit_stops = stop_limit1 is not None and stop_limit2 is not None
                coin1_limit_order = None
                coin2_limit_order = None
                if use_limit_stops:
                    coin2_limit_order = [o for o in BinanceAPI.get_all_account_orders(symbol=coin2)if o['clientOrderId'] == stop_limit2[orders_definition.clientOrderId]][0]
                    coin1_limit_order = [o for o in BinanceAPI.get_all_account_orders(symbol=coin1)if o['clientOrderId'] == stop_limit1[orders_definition.clientOrderId]][0]

                if close_time - market1[orders_definition.updateTime] > IntervalConverter.interval_to_time(ConfigManager.config['order_stop_limit_time']) * 1000:
                    #close trades via market and exit
                    close_market_order(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'TIME_STOP', pair)
                    continue
                
                if use_limit_stops and (coin1_limit_order['executedQty'] != '0' or coin2_limit_order['executedQty'] != '0'):
                    print('LIMIT FILLED IN SOME WAY')
                    coin1_status = 'UNDEFINED'
                    coin2_status = 'UNDEFINED'

                    if coin1_limit_order['status'] == 'NEW':
                        coin1_status = 'NORMAL'            
                    elif coin1_limit_order['status'] == 'FILLED':
                        coin1_status = 'FILLED'


                    if coin2_limit_order['status'] == 'NEW':
                        coin2_status = 'NORMAL'            
                    elif coin2_limit_order['status'] == 'FILLED':
                        coin2_status = 'FILLED'

                    pass
                    close_market_order(((market1, coin1_current_price, coin1_limit_order, coin1_status), (market2, coin2_current_price, coin2_limit_order, coin2_status)), close_time, 'PRICE_STOP_LIMIT_EXCEEDED', pair)
                    Repository.log_message(f"Pair {market1[orders_definition.symbol]}/{market2[orders_definition.symbol]} exceed price stop limit with {market1[orders_definition.symbol]}: {market_exceed_limits[1]} and {market2[orders_definition.symbol]}: {market2_exceed_limits[1]}. HedgeId: {market1[orders_definition.hedgeId]}")
                elif is_program_shutdown_started:
                    print('SHUTDOWN CLOSING')
                    close_market_order(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'PROGRAM_CLOSURE', pair)  
                
                elif not use_limit_stops and (market_exceed_limits[0] or market2_exceed_limits[0]):
                    print('CLOSING WHEN USING NO LIMITS')
                    close_market_order(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'PRICE_STOP_LIMIT_EXCEEDED', pair)
                    Repository.log_message(f"Pair {market1[orders_definition.symbol]}/{market2[orders_definition.symbol]} exceed price stop limit with {market1[orders_definition.symbol]}: {market_exceed_limits[1]} and {market2[orders_definition.symbol]}: {market2_exceed_limits[1]}. HedgeId: {market1[orders_definition.hedgeId]}")
                #up: 1 short(sell), 2 long(buy)
                #down: 1 long(buy), 2 short(sell)
                #we entered when crossed upper deviation bound
                elif market1[orders_definition.side] == 'SELL':
                    #close markets if relation price is lower than linear regression bound
                    if current_relation_price <= linear_regression_bound:
                        print('REGRESSION')
                        close_market_order(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'LINEAR_REGRESSION_CROSSING', pair)
                #we entered when crossed lower deviation bound
                elif market1[orders_definition.side] == 'BUY':
                    #close markets if relation price is higher than linear regression bound
                    if current_relation_price >= linear_regression_bound:
                        print('REGRESSION')
                        close_market_order(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'LINEAR_REGRESSION_CROSSING', pair)
                
        except Exception as err: 
            Repository.log_message(str(err))    
            print(str(err))

        

        end_time = time.time()
        delta_time = end_time-start_time
        print(f"{datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S]')} UTC | DONE CALCULATING ORDERS PROFIT | Time Elapsed: {int(delta_time)}")
        time.sleep(max(MathHelper.ceil(60 - delta_time), 1))
        pass

def symbols_loop():
    #Repository.set_pairs_default_is_outside_deviation()
    if PRODUCTION_MODE:
        balance = BinanceAPI.get_production_balance()
        Repository.set_available_balance(balance['available_balance'])
        Repository.set_balance(balance['balance'])
    while True:
        start_time = time.time()
        pairs = Repository.get_pairs() 
        
        for pair in pairs:
            pair_start_time = time.time()
            #exit if cointegrations outdated
            d_time = int(time.time()*1000) - pair[7]
            if IntervalConverter.interval_to_time(ConfigManager.config['kline_interval'])*1000 < d_time:
                continue  

            
            coin1 = pair[0].split('/')[0]
            coin2 = pair[0].split('/')[1]

            is_outside_deviations = bool(pair[3]) 
            adf = pair[1]
            cointegration_treshold = ConfigManager.config['adf_value_threshold']

            #exit if active orders on coins
            if Repository.check_if_orders_available(coin1) or Repository.check_if_orders_available(coin2):
                continue
                
            if is_outside_deviations:
                #getting linear regression polynom coefficients and deviation value
                deviation_multiplier = ConfigManager.config['lin_reg_deviation']
                deviation = pair[4]
                coefficients_linear_regression = (pair[5], pair[6])
                coefficients_up = (pair[5], pair[6] + (deviation * deviation_multiplier))
                coefficients_down = (pair[5], pair[6] - (deviation * deviation_multiplier))
                epoch_time = time.time() * 1000
                upper_bound = MathHelper.calculate_polynom(epoch_time, coefficients_up)
                lower_bound = MathHelper.calculate_polynom(epoch_time, coefficients_down)
                linear_regression_bound = MathHelper.calculate_polynom(epoch_time, coefficients_linear_regression)
                #calculating pair relation at the moment
                coin1_price = BinanceAPI.get_ticker_price(coin1)
                coin2_price = BinanceAPI.get_ticker_price(coin2)

                
                pair_relation_price = coin1_price/coin2_price
                if pair_relation_price < upper_bound and pair_relation_price > lower_bound:
                    Repository.update_pair_is_outside_deviations(pair=pair[0], is_outside_deviations=0)
                    if not Repository.get_is_order_creation_allowed():
                        continue
                    kotleta = 0 

                    #update kotleta values
                    if ONLINE_TEST_MODE:
                        kotleta = Repository.get_balance() 
                        if Repository.get_available_balance() < kotleta * 0.15:
                            continue
                    else:
                        balance = BinanceAPI.get_production_balance()
                        kotleta = balance['balance'] 
                        if balance['available_balance'] < kotleta * 0.20:
                            continue

                    coin1_info = Repository.get_currency(coin1)
                    coin1_max_notional = 1000000

                    coin2_info = Repository.get_currency(coin2)
                    coin2_max_notional = 1000000
                    leverage = min(coin1_info[5], coin2_info[5])
                    coin1_current_leverage, coin1_current_max_notional = Repository.get_current_leverage_and_max_notional(coin1)
                    if coin1_current_leverage != leverage:
                        coin1_max_notional = BinanceAPI.change_leverage(coin1, leverage)
                        Repository.update_current_leverage_and_max_notional(coin1, leverage, coin1_max_notional)

                    else:
                        coin1_max_notional = coin1_current_max_notional 
                        leverage = coin1_current_leverage
                    coin2_current_leverage, coin2_current_max_notional = Repository.get_current_leverage_and_max_notional(coin2)
                    if coin2_current_leverage != leverage:
                        coin2_max_notional = BinanceAPI.change_leverage(coin2, leverage)
                        Repository.update_current_leverage_and_max_notional(coin2, leverage, coin2_max_notional)
                    else:
                        coin2_max_notional = coin2_current_max_notional 
                        leverage = coin2_current_leverage
                    
                    stop_percentage = Repository.get_price_stop_percentage()

                    if pair_relation_price > linear_regression_bound:
                        long_stop_price = MathHelper.calculate_order_stop_price(current_price=coin2_price, 
                                                                                tick_size=coin2_info[4],
                                                                                stop_percentage=stop_percentage,
                                                                                leverage=leverage,
                                                                                side='BUY')
                        short_stop_price = MathHelper.calculate_order_stop_price(current_price=coin1_price, 
                                                                                tick_size=coin1_info[4],
                                                                                stop_percentage=stop_percentage,
                                                                                leverage=leverage,
                                                                                side='SELL')                                    
                        long_quantity = MathHelper.calculate_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin2_price,
                                                                      precision=coin2_info[2],
                                                                      minimum_notion=coin2_info[3],
                                                                      maximum_notion=coin2_max_notional,
                                                                      leverage=leverage)
                        short_quantity = MathHelper.calculate_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin1_price,
                                                                      precision=coin1_info[2],
                                                                      minimum_notion=coin1_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)
                        manage_market_order_creation(coin_long=coin2, quantity_long=long_quantity, long_current_price=coin2_price, long_stop_price=long_stop_price,
                                          coin_short=coin1, quantity_short=short_quantity, short_current_price=coin1_price, short_stop_price=short_stop_price, leverage=leverage)  
                        pass
                    else:
                        long_stop_price = MathHelper.calculate_order_stop_price(current_price=coin1_price, 
                                                                                tick_size=coin1_info[4],
                                                                                stop_percentage=stop_percentage,
                                                                                leverage=leverage,
                                                                                side='BUY')
                        short_stop_price = MathHelper.calculate_order_stop_price(current_price=coin2_price, 
                                                                                tick_size=coin2_info[4],
                                                                                stop_percentage=stop_percentage,
                                                                                leverage=leverage,
                                                                                side='SELL')   
                        long_quantity = MathHelper.calculate_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin1_price,
                                                                      precision=coin1_info[2],
                                                                      minimum_notion=coin1_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)

                        short_quantity = MathHelper.calculate_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin2_price,
                                                                      precision=coin2_info[2],
                                                                      minimum_notion=coin2_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)
                        manage_market_order_creation(coin_long=coin1, quantity_long=long_quantity, long_current_price=coin1_price, long_stop_price=long_stop_price,
                                          coin_short=coin2, quantity_short=short_quantity, short_current_price=coin2_price, short_stop_price=short_stop_price, leverage=leverage) 
                        pass
                    if PRODUCTION_MODE:
                        balance = BinanceAPI.get_production_balance()
                        Repository.set_available_balance(balance['available_balance'])
                        Repository.set_balance(balance['balance'])
                    #print('Exit due to orders placement|Time elapsed: {0}'.format(time.time() - pair_start_time)) 
                    continue
                pass
            


            #exit if pair not cointegrated
            if adf > cointegration_treshold:
                continue
            
            deviation_multiplier = ConfigManager.config['lin_reg_deviation']
            deviation = pair[4]
            coefficients_up = (pair[5], pair[6] + (deviation * deviation_multiplier))
            coefficients_down = (pair[5], pair[6] - (deviation * deviation_multiplier))
            epoch_time = time.time() * 1000
            upper_bound = MathHelper.calculate_polynom(epoch_time, coefficients_up)
            lower_bound = MathHelper.calculate_polynom(epoch_time, coefficients_down)

            pair_price_start_time = time.time()
            coin1_price = BinanceAPI.get_ticker_price(coin1)
            coin2_price = BinanceAPI.get_ticker_price(coin2)

            pair_relation_price = coin1_price/coin2_price
            if pair_relation_price > upper_bound or pair_relation_price < lower_bound:
                Repository.update_pair_is_outside_deviations(pair=pair[0], is_outside_deviations=1)
        end_time = time.time()
        delta_time = end_time-start_time
        print(f"{datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S]')} UTC | ALL PAIRS DONE | Time elapsed: {int(delta_time)}")
        if len(pairs) !=0:
            time.sleep(max(MathHelper.ceil(60/(LOOPS_PER_MINUTE/len(pairs)) - delta_time), 1))
        else:    
            time.sleep(max(MathHelper.ceil(60/(LOOPS_PER_MINUTE/50) - delta_time), 1))    
     
def update_symbols():
    config = ConfigManager.config

    cointegration = Repository.get_last_cointegration()
    if cointegration is not None: 
        d_time = int(time.time()) - cointegration[2]
        if IntervalConverter.interval_to_time(cointegration[0]) > d_time:
            print('Need to wait {0} seconds before next cointegration check'.format(IntervalConverter.interval_to_time(cointegration[0])-d_time))
            time.sleep(IntervalConverter.interval_to_time(cointegration[0])-d_time)
            pass
    while True:
        try: 
            t0 = time.time()
            Repository.delete_uncointegrated_pairs()           

            currencies=[c[0] for c in Repository.get_all_currencies()]
            Klines = BinanceAPI.get_klines(config['kline_interval'], config['bar_count'], currencies)
            
            #updating pairs
            pairs = [p[0] for p in Repository.get_pairs()] 
            pairs_to_update = []
            for pair in pairs:
                symbols = pair.split('/')
                klines1 = Klines.get(symbols[0])
                klines2 = Klines.get(symbols[1])
                close_relations=[double(k1[4])/double(k2[4]) for k1, k2 in zip(klines1, klines2)]
                close_times = [t[6] for t in klines1]

                deviation = MathHelper.calculate_deviation(close_relations)
                coefficients = MathHelper.calculate_linear_regression_coefficients(close_times,close_relations)
                adf = adfuller(close_relations)[0]
                is_not_cointegrated = (0, 1)[int(adf > config['adf_value_threshold'])]

                pairs_to_update.append((adf, is_not_cointegrated, deviation, coefficients[0], coefficients[1], int(time.time()*1000), pair))
            Repository.update_pairs(pairs_to_update)        

            ##########################    
            #adding new pairs
            pair_combinations = Repository.get_pair_combinations()
            pairs_to_insert = []
            for pair in pair_combinations:
                symbols = pair.split('/')
                klines1 = Klines.get(symbols[0])
                klines2 = Klines.get(symbols[1])
                close_relations=[double(k1[4])/double(k2[4]) for k1, k2 in zip(klines1, klines2)]

                hurst_exponent = compute_Hc(close_relations, kind='price', simplified=True)[0]
                if hurst_exponent >= 0.5:
                    continue
                adf = adfuller(close_relations)[0]
                if adf > ConfigManager.config['adf_value_threshold']:
                    continue       

                close_times = [t[6] for t in klines1]
                deviation = MathHelper.calculate_deviation(close_relations)
                coefficients = MathHelper.calculate_linear_regression_coefficients(close_times,close_relations)
                pairs_to_insert.append((pair, adf, 0, 0, deviation, coefficients[0], coefficients[1], int(time.time()*1000)))
            Repository.add_pairs(pairs_to_insert)    
              
            t_end = time.time()
            delta_time = int(t_end-t0)
            Repository.add_cointegration(bars=config['bar_count'],
                                        interval=config['kline_interval'],
                                        time_elapsed=delta_time,
                                        starting_time=int(t0))
            time_to_wait = IntervalConverter.interval_to_time(config['kline_interval']) - delta_time

            print('Done')

            BinanceAPI.update_currencies()
            Repository.delete_old_weights()
            time.sleep(time_to_wait)

        except Exception as err:
            print(str(err))
            Repository.log_message(str(err))            


if __name__ == '__main__':


    pass

    #p1 = Process(target=update_symbols)
    #p2 = Process(target=symbols_loop)
    #p3 = Process(target=trade_calculator)
    #p1.start()
    #p2.start()
    #p3.start()
    #p1.join()
    #p2.join()
    #p3.join()


pass









