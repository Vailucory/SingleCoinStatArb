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
LOOPS_PER_MINUTE = int(MAX_WEIGHT_PER_MINUTE/MAX_WEIGHT_PER_LOOP) 
ONLINE_TEST_MODE = False
BACKTEST_MODE = True
PRODUCTION_MODE = False
USE_VIRTUAL_KOTLETA = True
USE_MIN_REVERSION = True


config = ConfigManager.config
client = UMFutures(config['api_key'], config['api_secret'])
#client.base_url = "https://testnet.binancefuture.com/"


#klines = klines_1m[close_time]

#pair, adf, eg, deviation, lin_reg_coef_a, lin_reg_coef_b, time
def add_update_backtest_pairs_cointegrations(cointegrations):
    active_pairs = BacktestRepository.get_active_pairs_symbols()
    pairs_to_update = list([c for c in cointegrations if active_pairs.__contains__(c[0])])
    BacktestRepository.update_active_pairs(pairs_to_update)
    #pairs_to_add = list([c for c in cointegrations if c[1] <= config['adf_value_threshold'] and c[2] <= config['adf_value_threshold']])
    pairs_to_add = list([c for c in cointegrations if c[1] <= config['adf_value_threshold'] and c[6] < 0.5])
    BacktestRepository.add_active_pairs(pairs_to_add)

    pass

def run_backtest():
    #BacktestRepository.seed_database()
    close_times = BacktestRepository.get_backtest_klines1h_times()
    bar_count = ConfigManager.config['bar_count']
    interval = interval_to_time(ConfigManager.config['kline_interval']) * 1000
    coins = list([c[0] for c in BacktestRepository.get_all_currencies()])
    #1668617999999 1668621599999
    #index = close_times.index(1668621599999)
    #168-912
    start_index = 1196
    end_index = len(close_times)
    #BacktestRepository.set_burst_starting_balance(None)
    #is_burst_started = False
    for i in range(start_index, end_index):
        is_burst_started = BacktestRepository.get_burst_starting_balance() is not None
        if not is_burst_started:
            BacktestRepository.Execute("DELETE FROM active_pairs")
            BacktestRepository.set_burst_starting_balance(BacktestRepository.get_virtual_kotleta())
            BacktestRepository.set_is_order_creation_allowed(1)
            BacktestRepository.set_is_program_shutdown_started(0)
        start_logging_time = time.time()
        t0 = close_times[i - bar_count]
        t1 = close_times[i]
        t2 = t1 - interval

        cointegrations = BacktestRepository.get_cointegrations(t1)

        klines_1h = BacktestRepository.get_backtest_klines1h(t0, t1) 
        process_backtest_slice(t2,t1, cointegrations, coins, klines_1h)
        print('Done! | Time elapsed: {0} | {1}/{2}'.format(time.time()-start_logging_time, i, len(close_times)))
    
    max_klines_1m_time = BacktestRepository.get_max_klines_1m_time()
    last_klines_1h_time = close_times[end_index-1]
    current_klines_1m_time = last_klines_1h_time
    j = 0
    leftover_count = int((max_klines_1m_time-current_klines_1m_time)/interval)
    while current_klines_1m_time < max_klines_1m_time:


        j+=1
        process_backtest_slice(current_klines_1m_time,current_klines_1m_time+interval, None, None, None, False)
        current_klines_1m_time += interval
        print('DONE {0}/{1}'.format(j, leftover_count))
        pass
    pass

def is_coins_disabled(coin1:str, coin2:str)->bool:
    disabled_symbols = Repository.get_disabled_symbols()
    return disabled_symbols.__contains__(coin1) or disabled_symbols.__contains__(coin2)

def process_backtest_slice(start_time:int, end_time, cointegrations, coins, klines_1h, is_trade_creation_allowed=True):
    if is_trade_creation_allowed:
        if BacktestRepository.get_burst_starting_balance() is not None:
            add_update_backtest_pairs_cointegrations(cointegrations)
            BacktestRepository.delete_uncointegrated_pairs()
        else:
            add_update_backtest_pairs_cointegrations(cointegrations)
    klines_1m = BacktestRepository.get_backtest_klines1m(start_time, end_time)
    keys = list(klines_1m.keys())
    symbol = 0
    high = 1
    low = 2
    close = 3
    for close_time in keys:
        klines = klines_1m[close_time]

        #check active orders for completion
        coins_with_active_orders = BacktestRepository.get_coins_with_open_orders()

        

        if USE_MIN_REVERSION:
            klines_with_active_orders = {}
            [klines_with_active_orders.update({k[symbol]:k}) for k in klines if coins_with_active_orders.__contains__(k[symbol])]
            
            for order in BacktestRepository.get_all_orders():
                current_price = klines_with_active_orders[order[orders_definition.symbol]][close]
                current_profit = order[orders_definition.currentProfit] + MathHelper.calculate_order_profit(enter_price=order[orders_definition.lastPrice],
                                                                                                    triggered_price=current_price,
                                                                                                    quantity=order[orders_definition.origQty],
                                                                                                    order_type=order[orders_definition.side])
                BacktestRepository.add_to_kotleta(current_profit-order[orders_definition.currentProfit], close_time)
                BacktestRepository.update_order_current_parameters(current_profit=current_profit,
                                                                    last_price=current_price,
                                                                    symbol=order[orders_definition.symbol])
        
            burst_starting_balance = BacktestRepository.get_burst_starting_balance()
            availible_balance = BacktestRepository.get_virtual_kotleta()+sum([cp[0] for cp in BacktestRepository.ExecuteWithResult("SELECT currentProfit FROM orders;")])
            burst_win_percentage = (availible_balance - burst_starting_balance)/availible_balance * 100
            if burst_win_percentage > 10 or burst_win_percentage < -10:
                BacktestRepository.set_is_order_creation_allowed(0)
                BacktestRepository.set_is_program_shutdown_started(1)
            is_program_shutdown_started = BacktestRepository.get_is_program_shutdown_started()
            for coin1, coin2 in BacktestRepository.get_coins_with_open_orders_by_hedges():
                market1 = BacktestRepository.get_active_order_by_type(coin1, 'MARKET') 
                market2 = BacktestRepository.get_active_order_by_type(coin2, 'MARKET') 

                pair, deviation, lin_reg_coef_a, lin_reg_coef_b = BacktestRepository.get_pair_by_coins(market1[orders_definition.symbol], market2[orders_definition.symbol])


                coin1_current_price = klines_with_active_orders[market1[orders_definition.symbol]][close]
                coin2_current_price = klines_with_active_orders[market2[orders_definition.symbol]][close]

                if close_time - market1[orders_definition.updateTime] > interval_to_time(ConfigManager.config['min_reversion_stop_time']) * 1000:
                    #close trades via market and exit
                    close_market_orders(((market1, coin1_current_price), (market2, coin2_current_price)), close_time, 'TIME_STOP', pair)
                    continue
                #entering_relation_price = market1[orders_definition.price]/market2[orders_definition.price]
                current_relation_price = coin1_current_price/coin2_current_price
                #calculate bounds
                coefficients_linear_regression = (lin_reg_coef_a, lin_reg_coef_b)
                linear_regression_bound = MathHelper.calculate_polynom(close_time, coefficients_linear_regression)
                price_stop_percentage = BacktestRepository.get_price_stop_percentage()
                market1_exceed_limits = MathHelper.is_price_exceed_limit(market1[orders_definition.price], coin1_current_price, market1[orders_definition.leverage], price_stop_percentage, market1[orders_definition.side])
                market2_exceed_limits = MathHelper.is_price_exceed_limit(market2[orders_definition.price], coin2_current_price, market2[orders_definition.leverage], price_stop_percentage, market2[orders_definition.side])
                
                if is_program_shutdown_started:
                    close_market_orders(((market1, coin1_current_price), (market2, coin2_current_price)), close_time, 'PROGRAM_CLOSING', pair)  
                elif market1_exceed_limits[0] or market2_exceed_limits[0]:
                    close_market_orders(((market1, coin1_current_price), (market2, coin2_current_price)), close_time, 'PRICE_STOP_LIMIT_EXCEED', pair)
                    BacktestRepository.log_message(f"Pair {market1[orders_definition.symbol]}/{market2[orders_definition.symbol]} exceed price stop limit with {market1[orders_definition.symbol]}: {market1_exceed_limits[1]} and {market2[orders_definition.symbol]}: {market2_exceed_limits[1]}. HedgeId: {market1[orders_definition.hedgeId]}")
                #up: 1 short(sell), 2 long(buy)
                #down: 1 long(buy), 2 short(sell)
                #we entered when crossed upper deviation bound
                elif market1[orders_definition.side] == 'SELL':
                    #close markets if relation price is lower than linear regression bound
                    if current_relation_price <= linear_regression_bound:
                        close_market_orders(((market1, coin1_current_price), (market2, coin2_current_price)), close_time, 'LINEAR_REGRESSION_CROSSING', pair)
                #we entered when crossed lower deviation bound
                else:
                    #close markets if relation price is higher than linear regression bound
                    if current_relation_price >= linear_regression_bound:
                        close_market_orders(((market1, coin1_current_price), (market2, coin2_current_price)), close_time, 'LINEAR_REGRESSION_CROSSING', pair)
        
        if not is_trade_creation_allowed:
            continue
        if burst_win_percentage > 10 or burst_win_percentage < -10:
            
            BacktestRepository.set_burst_starting_balance(None)
            BacktestRepository.log_message(f"Burst ended with burst percentage {burst_win_percentage}")
            return
        active_pairs = BacktestRepository.get_active_pairs() 
        active_pairs_coins = []   
        for pair in active_pairs:
            active_pairs_coins.extend(pair[0].split('/'))   
        active_pairs_coins = unique(active_pairs_coins)
        active_pairs_coins_klines = {}
        [active_pairs_coins_klines.update({k[symbol]:k}) for k in klines if active_pairs_coins.__contains__(k[symbol])]
        #active_pairs_coins_klines = [{k[symbol]:k} for k in klines if active_pairs_coins.__contains__(k[symbol])]
        for pair in active_pairs:

            coin1 = pair[0].split('/')[0]
            coin2 = pair[0].split('/')[1]

            is_outside_deviations = bool(pair[3]) 
            adf = pair[1]
            cointegration_treshold = ConfigManager.config['adf_value_threshold']

            #exit if active orders on coins
            if BacktestRepository.check_if_orders_available(coin1) or BacktestRepository.check_if_orders_available(coin2):
                #print('Exit due to availible orders|BACKTEST|Time elapsed: {0}'.format(time.time() - pair_start_time))
                continue

            if is_outside_deviations:
                #getting linear regression polynom coefficients and deviation value
                deviation_multiplier = ConfigManager.config['lin_reg_deviation']
                deviation = pair[4]
                coefficients_linear_regression = (pair[5], pair[6])
                coefficients_up = (pair[5], pair[6] + (deviation * deviation_multiplier))
                coefficients_down = (pair[5], pair[6] - (deviation * deviation_multiplier))
                epoch_time = close_time
                upper_bound = MathHelper.calculate_polynom(epoch_time, coefficients_up)
                lower_bound = MathHelper.calculate_polynom(epoch_time, coefficients_down)
                linear_regression_bound = MathHelper.calculate_polynom(epoch_time, coefficients_linear_regression)
                #calculating pair relation at the moment
                coin1_price = active_pairs_coins_klines[coin1][close]
                coin2_price = active_pairs_coins_klines[coin2][close]


                
                
                pair_relation_price = coin1_price/coin2_price
                if pair_relation_price < upper_bound and pair_relation_price > lower_bound:
                    
                    
                    BacktestRepository.update_pair_is_outside_deviations(pair=pair[0], is_outside_deviations=0)
                    #1000
                    kotleta=0
                    if USE_VIRTUAL_KOTLETA:
                        kotleta = BacktestRepository.get_virtual_kotleta()
                        if BacktestRepository.get_kotleta() < kotleta * 0.15:
                            continue
                    else:
                        kotleta = BacktestRepository.get_kotleta()
                    if not BacktestRepository.get_is_order_creation_allowed():
                        continue

                    coin1_info = BacktestRepository.get_currency(coin1)
                    coin1_max_notional = 1000000

                    coin2_info = BacktestRepository.get_currency(coin2)
                    coin2_max_notional = 1000000
                    leverage = min(coin1_info[5], coin2_info[5])
                    if not BACKTEST_MODE:
                        coin1_max_notional = change_leverage(client, coin1, leverage)
                        coin2_max_notional = change_leverage(client, coin2, leverage)

                            

                    if pair_relation_price > linear_regression_bound:
                        long_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin2_price,
                                                                      precision=coin2_info[2],
                                                                      minimum_notion=coin2_info[3],
                                                                      maximum_notion=coin2_max_notional,
                                                                      leverage=leverage)
                        short_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin1_price,
                                                                      precision=coin1_info[2],
                                                                      minimum_notion=coin1_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)
                        place_hedge_order(coin_long=coin2, quantity_long=long_quantity, long_current_price=coin2_price, long_stop_price=None,
                                          coin_short=coin1, quantity_short=short_quantity, short_current_price=coin1_price, short_stop_price=None, leverage=leverage, backtest_time=close_time)  
                    else:
                        long_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin1_price,
                                                                      precision=coin1_info[2],
                                                                      minimum_notion=coin1_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)
                        short_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin2_price,
                                                                      precision=coin2_info[2],
                                                                      minimum_notion=coin2_info[3],
                                                                      maximum_notion=coin2_max_notional,
                                                                      leverage=leverage)
                        place_hedge_order(coin_long=coin1, quantity_long=long_quantity, long_current_price=coin1_price, long_stop_price=None,
                                          coin_short=coin2, quantity_short=short_quantity, short_current_price=coin2_price, short_stop_price=None, leverage=leverage, backtest_time=close_time) 
                        pass
                    continue
                pass
            
                  


            #exit if pair not cointegrated
            if adf > cointegration_treshold:
                #print('Exit due to coint treshold|Time elapsed: {0}'.format(time.time() - pair_start_time))
                continue
            
            

            deviation_multiplier = ConfigManager.config['lin_reg_deviation']
            deviation = pair[4]
            coefficients_up = (pair[5], pair[6] + (deviation * deviation_multiplier))
            coefficients_down = (pair[5], pair[6] - (deviation * deviation_multiplier))
            epoch_time = close_time
            upper_bound = MathHelper.calculate_polynom(epoch_time, coefficients_up)
            lower_bound = MathHelper.calculate_polynom(epoch_time, coefficients_down)


            coin1_price = active_pairs_coins_klines[coin1][close]
            coin2_price = active_pairs_coins_klines[coin2][close]
            
            pair_relation_price = coin1_price/coin2_price
            if pair_relation_price > upper_bound or pair_relation_price < lower_bound:
                BacktestRepository.update_pair_is_outside_deviations(pair=pair[0], is_outside_deviations=1)        
        pass
        


    pass

def close_market_orders(orders, exit_time, type, pair):
    
    if PRODUCTION_MODE:
        #TODO
        # handle slippage properly                                    
        
        for market, current_price, limit_order, status in orders:
            leverage = market[orders_definition.leverage]

            coin_current_leverage, coin_current_max_notional = Repository.get_current_leverage_and_max_notional(market[orders_definition.symbol])
            if coin_current_leverage != leverage:
                coin_max_notional = change_leverage(client, market[orders_definition.symbol], leverage)
                Repository.update_current_leverage_and_max_notional(market[orders_definition.symbol], leverage, coin_max_notional)        
            else:
                coin_max_notional = coin_current_max_notional 
                leverage = coin_current_leverage
            
            side = 'SELL' if market[orders_definition.side] == 'BUY' else 'BUY'
            if status == 'NORMAL':
                print('NORMAL')
                if limit_order is None or cancel_order(client, market[orders_definition.symbol], limit_order['orderId']):
                    #close by market if limit was not triggered 
                    print('NORMAL AFTER CLOSING')
                    order_market = place_api_order(client = client,
                                                symbol = market[orders_definition.symbol],
                                                side = side,
                                                reduceOnly=True,
                                                quantity = market[orders_definition.origQty])
            #elif status == 'FILLED_PARTIALLY':
            #    print('FILLED_PARTIALLY')
            #    if cancel_order(client, market[orders_definition.symbol], limit_order['orderId']):
            #        #close by market partially if limit was not triggered
            #        print('FILLED_PARTIALLY AFTER CLOSING')
            #        coin_quantity_precision = Repository.get_currency(market[orders_definition.symbol])[2]
            #        qty_to_fill = round(market[orders_definition.origQty] - double(limit_order['executedQty']), coin_quantity_precision)
            #        order_market = place_api_order(client = client,
            #                                    symbol = market[orders_definition.symbol],
            #                                    side = side,
            #                                    reduceOnly=True,
            #                                    quantity = qty_to_fill)
            elif status == 'FILLED':
                print('FILLED')
                #already closed position with limit
                continue
            pass
        

        time.sleep(5)        
        for market, current_price, limit_order, status in orders:

            


            trades = get_account_trades(client, market[orders_definition.symbol])
            

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
            Repository.delete_pair(pair)                        
        balance = get_production_balance(client)
        Repository.set_kotleta(balance['available_balance'])
        Repository.set_virtual_kotleta(balance['balance'])
        return

    for market, current_price in orders:
        if BACKTEST_MODE:
            #profit = MathHelper.calculate_order_profit(enter_price=market[orders_definition.price],
            #                                                    triggered_price=current_price,
            #                                                    quantity=market[orders_definition.origQty],
            #                                                    order_type=market[orders_definition.side])- MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
            profit = market[orders_definition.currentProfit] - MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
            BacktestRepository.add_backtest_result(symbol=market[orders_definition.symbol],
                                        triggered_order_type=type,
                                        profit=profit-MathHelper.calculate_entering_market_commission(enter_price=market[orders_definition.price], quantity=market[orders_definition.origQty]),
                                        exit_time=exit_time,
                                        enter_time=market[orders_definition.updateTime],
                                        tradeId=market[orders_definition.tradeId],
                                        hedgeId=market[orders_definition.hedgeId])
            BacktestRepository.remove_orders(market[orders_definition.symbol])
            #we need to discard last two fields
            BacktestRepository.archive_order(market[:orders_definition.lastPrice])

            exit_commission = -MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
            BacktestRepository.set_kotleta(BacktestRepository.get_kotleta()+market[orders_definition.origQty]/market[orders_definition.leverage]*market[orders_definition.price]+exit_commission, exit_time)
            if USE_VIRTUAL_KOTLETA:
                BacktestRepository.add_to_virtual_kotleta(profit, exit_time)
        elif ONLINE_TEST_MODE:
            #profit = MathHelper.calculate_order_profit(enter_price=market[orders_definition.price],
            #                                                    triggered_price=current_price,
            #                                                    quantity=market[orders_definition.origQty],
            #                                                    order_type=market[orders_definition.side]) - MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
            profit = market[orders_definition.currentProfit] - MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
            Repository.add_test_result(symbol=market[orders_definition.symbol],
                                        triggered_order_type=type,
                                        profit=profit-MathHelper.calculate_entering_market_commission(enter_price=market[orders_definition.price], quantity=market[orders_definition.origQty]),
                                        enter_price=market[orders_definition.price],
                                        exit_price=current_price,
                                        enter_time=market[orders_definition.updateTime],
                                        exit_time=exit_time,
                                        tradeId=market[orders_definition.tradeId],
                                        hedgeId=market[orders_definition.hedgeId],
                                        leverage=market[orders_definition.leverage])
            Repository.remove_orders(market[orders_definition.symbol])
            #we need to discard 2 last fields
            Repository.archive_order(market[:orders_definition.lastPrice])                            
            Repository.set_kotleta(Repository.get_kotleta()+market[orders_definition.origQty]/market[orders_definition.leverage]*market[orders_definition.price]+profit)
            if USE_VIRTUAL_KOTLETA:
                exit_commission = -MathHelper.calculate_exiting_market_commission(current_price, market[orders_definition.origQty])
                Repository.add_to_virtual_kotleta(exit_commission)
        
    if BACKTEST_MODE:
        BacktestRepository.delete_pair(pair)
    else:
        Repository.delete_pair(pair)


def split(a:list, n:int):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

def manage_backtest_cointegrations():
    split_count = 6
    
    pass
    #pairs = list(split(BacktestRepository.get_backtest_uncalculated_pairs(),split_count))
    pairs = list(split([p[0] for p in BacktestRepository.ExecuteWithResult("Select pair FROM(SELECT pair, count(*) as c from cointegrations INNER JOIN pairs ON pairs.id == cointegrations.pairId GROUP BY pairId) WHERE c == 552")],split_count))
    #pairs = list(split(['BTCUSDT/ETHUSDT'],split_count))
    processes = []
    for i in range(split_count):
        processes.append(Process(target=get_pairs_cointegrations, args=list(pairs[i])))
    for p in processes:
        p.start()
    for p in processes:
        p.join()    


    BacktestRepository.Execute("UPDATE cointegrations SET adf=0 WHERE adf IS NULL")
    pass
     

    
    pass

def get_pairs_cointegrations(*args):
    i=1
    for p in args: 
        print('{0}/{1}'.format(i, len(args)))
        i +=1
        pair = p.split('/')
        coin1 = pair[0]
        bars1 = BacktestRepository.get_backtest_klines1h_by_symbol(coin1)
        #bars1 = BacktestRepository.get_backtest_klines1m_by_symbol(coin1)
        coin2 = pair[1]
        bars2 = BacktestRepository.get_backtest_klines1h_by_symbol(coin2)
        #bars2 = BacktestRepository.get_backtest_klines1m_by_symbol(coin2)
        get_pair_cointegrations(coin1, bars1, coin2, bars2)

def get_pair_cointegrations(coin1:str, bars1, coin2:str, bars2):
    if len(bars1) != len(bars2):
        raise Exception('bars count is not equal') 
    try:
        result = []
        bar_count = ConfigManager.config['bar_count']
        #bar_count = 240
        relation_bars_closes = [b1[5]/b2[5] for b1, b2 in zip(bars1, bars2, strict=True)]
        bars_time = [b[6] for b in bars1]
        pair_id = BacktestRepository.get_backtest_pairId(coin1, coin2)
        for i in range(len(bars1)-bar_count):
            if bars1[i+bar_count][6] <= 1674554399999:
                continue
            slice_relation = relation_bars_closes[i:i+bar_count]
            df = adfuller(slice_relation)
            deviation = MathHelper.calculate_deviation(slice_relation)
            coefs = MathHelper.calculate_linear_regression_coefficients(bars_time[i:i+bar_count],slice_relation)
            hurst_exp = compute_Hc(relation_bars_closes, kind='price', simplified=True)[0]
            result.append((bars1[i+bar_count][6], df[0], 0, deviation, coefs[0], coefs[1], hurst_exp, 3600,  pair_id))
            pass
        BacktestRepository.add_backtest_cointegrations(result)    
 
    except Exception as err: get_pair_cointegrations(coin1, bars1, coin2, bars2)



def seed_backtest_klines():
    config = ConfigManager.config
    client = UMFutures(config['api_key'], config['api_secret'])


    


    bar_count = config['bar_count']
    interval = interval_to_time(config['kline_interval'])
    t = bar_count * interval

    #make smart klines loading
    start_date = datetime(year=2023, month=1, day=24, hour=12)
    end_date = datetime(year=2023, month=1, day=31, hour=23)
    
    #start_date = datetime(year=2022, month=10, day=10, hour=23)
    #end_date = datetime(year=2022, month=10, day=18, hour=23)
    #start_date = datetime(year=2022, month=10, day=18, hour=23)
    #end_date = datetime(year=2022, month=11, day=25, hour=23)
    #start_date = datetime(year=2022, month=11, day=25, hour=23)
    #end_date = datetime(year=2022, month=12, day=19, hour=23)

    currencies = [c[0] for c in BacktestRepository.get_all_currencies()]
    #comb = list(combinations(currencies, 2))
    #i=1
    #for c in comb:
    #    print('{0} / {1}'.format(i, len(comb)))
    #    i +=1
    #    #s1 = c[0]
    #    #s2 = c[1]
    #    BacktestRepository.Execute("INSERT INTO pairs(pair) VALUES('{0}/{1}')".format(c[0], c[1]))
    limit = 1500
    
    end_time = int(datetime.timestamp(end_date)*1000)
    i = 1
    for symbol in currencies:

        log_time_start = time.time() 

        start_time = int(datetime.timestamp(start_date)*1000)
        start_time_1h = start_time# - config['bar_count'] * 3600000
        end_time_1h = end_time

        print('Loading {0} | {1} from {2}'.format(symbol, i, len(currencies)))
        #klines_1h = []
        #i += 1
        #while True:
        #    kline = []
        #    while True:
        #        if is_api_call_possible(KLINES_1000_1500_WEIGHT):
        #            t = time.time() * 1000
        #            Repository.add_weight(weight=KLINES_1000_1500_WEIGHT, method='klines_1500', time=t)
        #            kline = [k for k in client.klines(symbol=symbol, interval='1h', startTime=start_time_1h, limit= limit) if k[0]< end_time_1h]
        #            Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
        #            break
        #        #wait 1 second and try to repeat
        #        time.sleep(1)
        #    klines_1h.extend(kline)
        #    if len(kline) != 0:
        #        start_time_1h = int(kline[-1][0])+1
        #    if len(kline) < limit:
        #        break  
        #print('Loaded 1h for {0} | Time elapsed: {1}'.format(symbol, time.time()-log_time_start))
        #log_time_start = time.time() 
        #tmp = klines_1h[-1]            
        #BacktestRepository.add_klines(symbol,klines_1h,'1h')

        start_time = int(datetime.timestamp(start_date)*1000)
        start_time_1m = 1672570799999-3600000*2#start_time + 24 * 3600000#- config['bar_count'] * 3600000
        end_time_1m = 1676481719000#end_time + 24 * 3600000

        klines_1m = []
        while True:
            kline = []
            while True:
                if is_api_call_possible(KLINES_1000_1500_WEIGHT):
                    t = time.time() * 1000
                    Repository.add_weight(weight=KLINES_1000_1500_WEIGHT, method='klines_1500', time=t)
                    kline = [k for k in client.klines(symbol=symbol, interval='1m', startTime=start_time_1m, limit= limit) if k[0]< end_time_1m]
                    Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                    break
                #wait 1 second and try to repeat
                time.sleep(1)
            klines_1m.extend(kline)
            if len(kline) != 0:
                start_time_1m = int(kline[-1][0])+1
            if len(kline) < limit:
                break  
        print('Loaded 1m for {0} | Time elapsed: {1}'.format(symbol, time.time()-log_time_start))
        tmp = klines_1m[-1]
        BacktestRepository.add_klines(symbol,klines_1m,'1m')
        print(len(klines_1m))
        #start_time = 1670101199999 
        #end_time = 1671573599999 + 60000
        #klines_1m = []
        #while True:
        #    kline = []
        #    while True:
        #        if is_api_call_possible(KLINES_1000_1500_WEIGHT):
        #            t = time.time() * 1000
        #            Repository.add_weight(weight=KLINES_1000_1500_WEIGHT, method='klines_1500', time=t)
        #            kline = [k for k in client.klines(symbol=symbol, interval='1m', startTime=start_time, limit= limit) if k[0]< end_time]
        #            Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
        #            break
        #        #wait 1 second and try to repeat
        #        time.sleep(1)
        #    klines_1m.extend(kline)
        #    if len(kline) != 0:
        #        start_time = int(kline[-1][0])+1
        #    if len(kline) < limit:
        #        break  
        #print('Loaded 1h for {0} | Time elapsed: {1}'.format(symbol, time.time()-log_time_start))
        #log_time_start = time.time()          
        #BacktestRepository.add_klines(symbol,klines_1m,'1m')
        #i+=1
    #BacktestRepository.Execute("DELETE FROM klines_1h WHERE close_time > {}".format(end_time))
    #BacktestRepository.Execute("DELETE FROM klines_1m WHERE close_time > {}".format(end_time))
    pass


def is_api_call_possible(weight:int) -> bool:
    return (Repository.get_weight_for_last_minute(int(time.time()*1000)) + weight) < MAX_WEIGHT_PER_MINUTE

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

def get_production_balance(client:UMFutures):
    while True:
            if is_api_call_possible(BALANCE_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=BALANCE_WEIGHT, method='balance', time=t)
                balance = [{'balance':double(entry['balance']), 'available_balance':double(entry['availableBalance'])} for entry in client.balance() if entry['asset'] == 'USDT'][0]
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return balance
            #wait 1 second and try to repeat
            time.sleep(1)

def place_api_order(client:UMFutures, symbol:str, quantity:double, side:str, reduceOnly:bool=False, max_retries=40):
    i = 0
    while True:
        try:
            i += 1
            order = client.new_order(symbol=symbol,
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

def cancel_order(client:UMFutures, symbol:str, orderId:int, max_retries=40) -> bool:
    print('cancel_order start')
    i = 0
    while True:
        try:
            i += 1
            while True:
                if is_api_call_possible(CANCEL_ORDER_WEIGHT):
                    t = time.time() * 1000
                    Repository.add_weight(weight=CANCEL_ORDER_WEIGHT, method='cancel_order', time=t)
                    client.cancel_order(symbol, orderId = orderId)
                    Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                    print('cancel_order True')
                    return True
                #wait 1 second and try to repeat
                time.sleep(1)
        except ApiErrors.ServerError as e:
            if i >= max_retries:
                Repository.log_message(f"ERROR CANCELLING LIMIT | {str(e)}")    
                raise
            time.sleep(15)
        except ApiErrors.ClientError as e:
            #order allready closed
            if e.error_code == -2011:
                print('cancel_order False')
                return False
            raise 

def place_api_market_stop_order(client:UMFutures, symbol:str, stop_price:double, side:str, max_retries=40):
    i = 0
    while True:
        try:
            i += 1
            order = client.new_order(symbol=symbol,
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

def get_ticker_price(client:UMFutures, coin):
    while True:
            if is_api_call_possible(TICKER_PRICE_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=TICKER_PRICE_WEIGHT, method='ticker_price', time=t)
                price = double(client.ticker_price(coin)['price'])
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return price
            #wait 1 second and try to repeat
            time.sleep(1)

def get_account_trades(client:UMFutures, symbol):
    while True:
            if is_api_call_possible(GET_TRADES_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=GET_TRADES_WEIGHT, method='get_account_trades', time=t)
                trades = client.get_account_trades(symbol)
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return trades
            #wait 1 second and try to repeat
            time.sleep(1)

def get_all_account_orders(client:UMFutures, symbol):
    while True:
            if is_api_call_possible(GET_SYMBOL_ORDERS_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=GET_SYMBOL_ORDERS_WEIGHT, method='get_orders', time=t)
                orders = client.get_all_orders(symbol=symbol)
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                return orders
            #wait 1 second and try to repeat
            time.sleep(1)

def change_leverage(client, coin, leverage):
    while True:
        if is_api_call_possible(CHANGE_LEVERAGE_WEIGHT):
            t = time.time() * 1000    
            Repository.add_weight(weight=CHANGE_LEVERAGE_WEIGHT, method='change_leverage', time=t)
            max_notional = int(client.change_leverage(coin, leverage)['maxNotionalValue'])
            Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
            return max_notional
        #wait 1 second and try to repeat
        time.sleep(1)


def place_hedge_order(coin_long, quantity_long, long_current_price, long_stop_price, 
                      coin_short, quantity_short, short_current_price, short_stop_price, leverage = 20, backtest_time = None):
    


    long_order_id = int(time.time() * 1000)
    hedgeId = int(time.time() * 1000)
    
    
    try:
        if ONLINE_TEST_MODE:
            Repository.set_kotleta(MathHelper.calculate_balance_after_entering_market(Repository.get_kotleta(), long_current_price, quantity_long, leverage))
        if BACKTEST_MODE:
            BacktestRepository.set_kotleta(MathHelper.calculate_balance_after_entering_market(BacktestRepository.get_kotleta(), long_current_price, quantity_long, leverage), backtest_time)
        if USE_VIRTUAL_KOTLETA:
            if BACKTEST_MODE:
                BacktestRepository.set_virtual_kotleta(BacktestRepository.get_virtual_kotleta()-MathHelper.calculate_entering_market_commission(long_current_price, quantity_long), backtest_time)
            elif ONLINE_TEST_MODE:
                Repository.set_virtual_kotleta(Repository.get_virtual_kotleta()-MathHelper.calculate_entering_market_commission(long_current_price, quantity_long))
        #if BACKTEST_MODE or ONLINE_TEST_MODE:   
        #    quantity_long *= leverage
        order_long_market = {}
        stop_order_long = {}
        
        if ONLINE_TEST_MODE:
            order_long_market = place_api_order(client=client,
                                                symbol=coin_long,
                                                side='BUY',
                                                quantity=quantity_long)
        elif PRODUCTION_MODE:
            
            order_long_market = place_api_order(client=client,
                                                symbol=coin_long,
                                                side='BUY',
                                                quantity=quantity_long)
            stop_order_long = place_api_market_stop_order(client=client,
                                                    symbol=coin_long,
                                                    stop_price=long_stop_price,
                                                    side='SELL')                                    
        elif BACKTEST_MODE:
            fill_backtest_order_with_defaults(order_long_market)

        if BACKTEST_MODE or ONLINE_TEST_MODE:
            order_long_market['symbol'] = coin_long
            order_long_market['orderId'] = random.randint(1, 9999999)
            order_long_market['status'] = 'NOT EXIST'
            order_long_market['clientOrderId'] = 0
            order_long_market['price'] = long_current_price
            order_long_market['avgPrice'] = long_current_price
            order_long_market['origQty'] = quantity_long
            order_long_market['executedQty'] = 0
            order_long_market['cumQuote'] = 0
            order_long_market['timeInForce'] = 'GTC'
            order_long_market['type'] = 'MARKET'
            order_long_market['reduceOnly'] = '0'
            order_long_market['closePosition'] = '0'
            order_long_market['side'] = 'BUY'
            order_long_market['positionSide'] = 'BOTH'
            order_long_market['stopPrice'] = 0
            order_long_market['workingType'] = 'CONTRACT_PRICE'
            order_long_market['priceProtect'] = 0
            order_long_market['origType'] = 'MARKET'
            order_long_market['updateTime'] = int(time.time()*1000)
        
        order_long_market.update({'tradeId':long_order_id})
        order_long_market.update({'hedgeId':hedgeId})
        order_long_market.update({'leverage':leverage})
        stop_order_long.update({'tradeId':long_order_id})
        stop_order_long.update({'hedgeId':hedgeId})
        stop_order_long.update({'leverage':leverage})

        if BACKTEST_MODE or ONLINE_TEST_MODE:
            order_long_market.update({'lastPrice':long_current_price})
            order_long_market.update({'currentProfit':0})

        if BACKTEST_MODE:
            order_long_market['updateTime'] = backtest_time
            BacktestRepository.add_order(order_long_market)
        elif ONLINE_TEST_MODE:
            Repository.add_test_order(order_long_market)
        else:
            order_long_market['price'] = long_current_price
            Repository.add_order(order_long_market)
            Repository.add_order(stop_order_long)

    except Exception as err: 
        if BACKTEST_MODE:
            BacktestRepository.log_message(coin_long + str(err))    
        else:
            Repository.log_message(coin_long + str(err))    

    short_order_id = int(time.time() * 1000)
    try:
        if ONLINE_TEST_MODE:
            Repository.set_kotleta(MathHelper.calculate_balance_after_entering_market(Repository.get_kotleta(), short_current_price, quantity_short, leverage))
        if BACKTEST_MODE:
            BacktestRepository.set_kotleta(MathHelper.calculate_balance_after_entering_market(BacktestRepository.get_kotleta(), short_current_price, quantity_short, leverage), backtest_time)
        if USE_VIRTUAL_KOTLETA:
            if BACKTEST_MODE:
                BacktestRepository.set_virtual_kotleta(BacktestRepository.get_virtual_kotleta()-MathHelper.calculate_entering_market_commission(short_current_price, quantity_short), backtest_time)
            elif ONLINE_TEST_MODE:
                Repository.set_virtual_kotleta(Repository.get_virtual_kotleta()-MathHelper.calculate_entering_market_commission(short_current_price, quantity_short))
        #if BACKTEST_MODE or ONLINE_TEST_MODE:       
        #    quantity_short *= leverage
        order_short_market = {}
        stop_order_short = {}

        if ONLINE_TEST_MODE:
            order_short_market = place_api_order(client=client,
                                                symbol=coin_short,
                                                side='SELL',
                                                quantity=quantity_short)
        elif PRODUCTION_MODE:
            order_short_market = place_api_order(client=client,
                                                symbol=coin_short,
                                                side='SELL',
                                                quantity=quantity_short)
            stop_order_short = place_api_market_stop_order(client=client,
                                                    symbol=coin_short,
                                                    stop_price=short_stop_price,
                                                    side='BUY') 
        elif BACKTEST_MODE:
            fill_backtest_order_with_defaults(order_short_market)
        
        if BACKTEST_MODE or ONLINE_TEST_MODE:    
            order_short_market['symbol'] = coin_short
            order_short_market['orderId'] = random.randint(1, 9999999)
            order_short_market['status'] = 'NOT EXIST'
            order_short_market['clientOrderId'] = 0
            order_short_market['price'] = short_current_price
            order_short_market['avgPrice'] = short_current_price
            order_short_market['origQty'] = quantity_short
            order_short_market['executedQty'] = 0
            order_short_market['cumQuote'] = 0
            order_short_market['timeInForce'] = 'GTC'
            order_short_market['type'] = 'MARKET'
            order_short_market['reduceOnly'] = '0'
            order_short_market['closePosition'] = '0'
            order_short_market['side'] = 'SELL'
            order_short_market['positionSide'] = 'BOTH'
            order_short_market['stopPrice'] = 0
            order_short_market['workingType'] = 'CONTRACT_PRICE'
            order_short_market['priceProtect'] = 0
            order_short_market['origType'] = 'MARKET'
            order_short_market['updateTime'] = int(time.time()*1000)

        order_short_market.update({'tradeId':short_order_id})
        order_short_market.update({'hedgeId':hedgeId})
        order_short_market.update({'leverage':leverage})
        stop_order_short.update({'tradeId':short_order_id})
        stop_order_short.update({'hedgeId':hedgeId})
        stop_order_short.update({'leverage':leverage})
        
        if BACKTEST_MODE or ONLINE_TEST_MODE:
            order_short_market.update({'lastPrice':short_current_price})
            order_short_market.update({'currentProfit':0})


        if BACKTEST_MODE:
            order_short_market['updateTime'] = backtest_time
            BacktestRepository.add_order(order_short_market)
        elif ONLINE_TEST_MODE:
            Repository.add_test_order(order_short_market)
        else:
            order_short_market['price'] = short_current_price
            Repository.add_order(order_short_market)
            Repository.add_order(stop_order_short)

    except Exception as err: 
        if BACKTEST_MODE:
            BacktestRepository.log_message(coin_short + str(err))    
        else:
            Repository.log_message(coin_short + str(err))    
    
    if ONLINE_TEST_MODE or PRODUCTION_MODE:
        Repository.set_hedges_launched(Repository.get_hedges_launched()+1)
        if Repository.get_hedges_launched() >= Repository.get_backtest_hedge_limit() and Repository.get_backtest_hedge_limit() != -1:
            Repository.set_is_order_creation_allowed(False)
    if BACKTEST_MODE: 
        BacktestRepository.set_hedges_launched(BacktestRepository.get_hedges_launched()+1)
        if BacktestRepository.get_hedges_launched() >= BacktestRepository.get_backtest_hedge_limit() and BacktestRepository.get_backtest_hedge_limit() != -1:
            BacktestRepository.set_is_order_creation_allowed(False)      
    pass
 
   


def trade_calculator_loop():
    config = ConfigManager.config
    client = UMFutures(config['api_key'], config['api_secret'])
    while True:
        try:
            start_time = time.time()
            is_program_shutdown_started = Repository.get_is_program_shutdown_started()
            if ONLINE_TEST_MODE:
                for order in Repository.get_all_market_orders():
                    current_price = get_ticker_price(client, order[orders_definition.symbol])
                    
                    
                    current_profit = order[orders_definition.currentProfit] + MathHelper.calculate_order_profit(enter_price=order[orders_definition.lastPrice],
                                                                                                        triggered_price=current_price,
                                                                                                        quantity=order[orders_definition.origQty],
                                                                                                        order_type=order[orders_definition.side])
                    Repository.add_to_virtual_kotleta(current_profit-order[orders_definition.currentProfit])
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

                coin1_current_price = get_ticker_price(client, coin1)
                coin2_current_price = get_ticker_price(client, coin2)
                
                
                   
                current_relation_price = coin1_current_price/coin2_current_price
                coefficients_linear_regression = (lin_reg_coef_a, lin_reg_coef_b)
                linear_regression_bound = MathHelper.calculate_polynom(close_time, coefficients_linear_regression)  
                price_stop_percentage = Repository.get_price_stop_percentage()
                market1_exceed_limits = MathHelper.is_price_exceed_limit(market1[orders_definition.price], coin1_current_price, market1[orders_definition.leverage], price_stop_percentage, market1[orders_definition.side])
                market2_exceed_limits = MathHelper.is_price_exceed_limit(market2[orders_definition.price], coin2_current_price, market2[orders_definition.leverage], price_stop_percentage, market2[orders_definition.side])
                use_limit_stops = stop_limit1 is not None and stop_limit2 is not None
                coin1_limit_order = None
                coin2_limit_order = None
                if use_limit_stops:
                    coin2_limit_order = [o for o in get_all_account_orders(client=client, symbol=coin2)if o['clientOrderId'] == stop_limit2[orders_definition.clientOrderId]][0]
                    coin1_limit_order = [o for o in get_all_account_orders(client=client, symbol=coin1)if o['clientOrderId'] == stop_limit1[orders_definition.clientOrderId]][0]

                if close_time - market1[orders_definition.updateTime] > interval_to_time(ConfigManager.config['min_reversion_stop_time']) * 1000:
                    #close trades via market and exit
                    close_market_orders(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'TIME_STOP', pair)
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
                    close_market_orders(((market1, coin1_current_price, coin1_limit_order, coin1_status), (market2, coin2_current_price, coin2_limit_order, coin2_status)), close_time, 'PRICE_STOP_LIMIT_EXCEED', pair)
                    Repository.log_message(f"Pair {market1[orders_definition.symbol]}/{market2[orders_definition.symbol]} exceed price stop limit with {market1[orders_definition.symbol]}: {market1_exceed_limits[1]} and {market2[orders_definition.symbol]}: {market2_exceed_limits[1]}. HedgeId: {market1[orders_definition.hedgeId]}")
                elif is_program_shutdown_started:
                    print('SHUTDOWN CLOSING')
                    close_market_orders(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'PROGRAM_CLOSING', pair)  
                
                elif not use_limit_stops and (market1_exceed_limits[0] or market2_exceed_limits[0]):
                    print('CLOSING WHEN USING NO LIMITS')
                    close_market_orders(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'PRICE_STOP_LIMIT_EXCEED', pair)
                    Repository.log_message(f"Pair {market1[orders_definition.symbol]}/{market2[orders_definition.symbol]} exceed price stop limit with {market1[orders_definition.symbol]}: {market1_exceed_limits[1]} and {market2[orders_definition.symbol]}: {market2_exceed_limits[1]}. HedgeId: {market1[orders_definition.hedgeId]}")
                #up: 1 short(sell), 2 long(buy)
                #down: 1 long(buy), 2 short(sell)
                #we entered when crossed upper deviation bound
                elif market1[orders_definition.side] == 'SELL':
                    #close markets if relation price is lower than linear regression bound
                    if current_relation_price <= linear_regression_bound:
                        print('REGRESSION')
                        close_market_orders(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'LINEAR_REGRESSION_CROSSING', pair)
                #we entered when crossed lower deviation bound
                elif market1[orders_definition.side] == 'BUY':
                    #close markets if relation price is higher than linear regression bound
                    if current_relation_price >= linear_regression_bound:
                        print('REGRESSION')
                        close_market_orders(((market1, coin1_current_price, coin1_limit_order, 'NORMAL'), (market2, coin2_current_price, coin2_limit_order, 'NORMAL')), close_time, 'LINEAR_REGRESSION_CROSSING', pair)
                
        except Exception as err: 
            Repository.log_message(str(err))    
            print(str(err))

        

        end_time = time.time()
        delta_time = end_time-start_time
        print(f"{datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S]')} UTC | DONE CALCULATING ORDERS PROFIT | Time Elapsed: {int(delta_time)}")
        time.sleep(max(MathHelper.ceil(60 - delta_time), 1))
        pass

def pair_loop():
    #Repository.set_pairs_default_is_outside_deviation()
    if PRODUCTION_MODE:
        balance = get_production_balance(client)
        Repository.set_kotleta(balance['available_balance'])
        Repository.set_virtual_kotleta(balance['balance'])
    while True:
        start_time = time.time()
        pairs = Repository.get_pairs() 
        
        for pair in pairs:
            pair_start_time = time.time()
            #exit if cointegrations outdated
            d_time = int(time.time()*1000) - pair[7]
            if interval_to_time(config['kline_interval'])*1000 < d_time:
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
                coin1_price = get_ticker_price(client, coin1)
                coin2_price = get_ticker_price(client, coin2)

                
                pair_relation_price = coin1_price/coin2_price
                if pair_relation_price < upper_bound and pair_relation_price > lower_bound:
                    Repository.update_pair_is_outside_deviations(pair=pair[0], is_outside_deviations=0)
                    if not Repository.get_is_order_creation_allowed():
                        continue
                    if is_coins_disabled(coin1, coin2):
                        continue
                    kotleta = 0 

                    #update kotleta values
                    if ONLINE_TEST_MODE:
                        kotleta = Repository.get_virtual_kotleta() if USE_VIRTUAL_KOTLETA else Repository.get_kotleta()
                        if Repository.get_kotleta() < kotleta * 0.15:
                            continue
                    else:
                        balance = get_production_balance(client)
                        kotleta = balance['balance'] if USE_VIRTUAL_KOTLETA else balance['available_balance']
                        if balance['available_balance'] < kotleta * 0.20:
                            continue

                    coin1_info = Repository.get_currency(coin1)
                    coin1_max_notional = 1000000

                    coin2_info = Repository.get_currency(coin2)
                    coin2_max_notional = 1000000
                    leverage = min(coin1_info[5], coin2_info[5])
                    coin1_current_leverage, coin1_current_max_notional = Repository.get_current_leverage_and_max_notional(coin1)
                    if coin1_current_leverage != leverage:
                        coin1_max_notional = change_leverage(client, coin1, leverage)
                        Repository.update_current_leverage_and_max_notional(coin1, leverage, coin1_max_notional)

                    else:
                        coin1_max_notional = coin1_current_max_notional 
                        leverage = coin1_current_leverage
                    coin2_current_leverage, coin2_current_max_notional = Repository.get_current_leverage_and_max_notional(coin2)
                    if coin2_current_leverage != leverage:
                        coin2_max_notional = change_leverage(client, coin2, leverage)
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
                        long_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin2_price,
                                                                      precision=coin2_info[2],
                                                                      minimum_notion=coin2_info[3],
                                                                      maximum_notion=coin2_max_notional,
                                                                      leverage=leverage)
                        short_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin1_price,
                                                                      precision=coin1_info[2],
                                                                      minimum_notion=coin1_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)
                        place_hedge_order(coin_long=coin2, quantity_long=long_quantity, long_current_price=coin2_price, long_stop_price=long_stop_price,
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
                        long_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin1_price,
                                                                      precision=coin1_info[2],
                                                                      minimum_notion=coin1_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)

                        short_quantity = MathHelper.calculate_min_reversion_quantity(
                                                                      total=kotleta,
                                                                      entry_price=coin2_price,
                                                                      precision=coin2_info[2],
                                                                      minimum_notion=coin2_info[3],
                                                                      maximum_notion=coin1_max_notional,
                                                                      leverage=leverage)
                        place_hedge_order(coin_long=coin1, quantity_long=long_quantity, long_current_price=coin1_price, long_stop_price=long_stop_price,
                                          coin_short=coin2, quantity_short=short_quantity, short_current_price=coin2_price, short_stop_price=short_stop_price, leverage=leverage) 
                        pass
                    if PRODUCTION_MODE:
                        balance = get_production_balance(client)
                        Repository.set_kotleta(balance['available_balance'])
                        if USE_VIRTUAL_KOTLETA:
                            Repository.set_virtual_kotleta(balance['balance'])
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
            coin1_price = get_ticker_price(client, coin1)
            coin2_price = get_ticker_price(client, coin2)

            pair_relation_price = coin1_price/coin2_price
            if pair_relation_price > upper_bound or pair_relation_price < lower_bound and not is_coins_disabled(coin1, coin2):
                Repository.update_pair_is_outside_deviations(pair=pair[0], is_outside_deviations=1)
        end_time = time.time()
        delta_time = end_time-start_time
        print(f"{datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S]')} UTC | ALL PAIRS DONE | Time elapsed: {int(delta_time)}")
        if len(pairs) !=0:
            time.sleep(max(MathHelper.ceil(60/(LOOPS_PER_MINUTE/len(pairs)) - delta_time), 1))
        else:    
            time.sleep(max(MathHelper.ceil(60/(LOOPS_PER_MINUTE/50) - delta_time), 1))    
     
def check_cointgrations():
    config = ConfigManager.config
    client = UMFutures(config['api_key'], config['api_secret'])
    cointegration = Repository.get_last_cointegration()
    if cointegration is not None: 
        d_time = int(time.time()) - cointegration[2]
        if interval_to_time(cointegration[0]) > d_time:
            print('Need to wait {0} seconds before next cointegration check'.format(interval_to_time(cointegration[0])-d_time))
            time.sleep(interval_to_time(cointegration[0])-d_time)
            pass
    while True:
        try: 
            t0 = time.time()
            Repository.delete_uncointegrated_pairs()           
            client = UMFutures(config['api_key'], config['api_secret'])
            currencies=[c[0] for c in Repository.get_all_currencies()]
            Klines = Get_Klines(client, currencies)
            
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
            time_to_wait = interval_to_time(config['kline_interval']) - delta_time

            print('Done')

            update_currencies()
            Repository.delete_old_weights()
            time.sleep(time_to_wait)

        except Exception as err:
            print(str(err))
            Repository.log_message(str(err))            

def interval_to_time(interval:str):
    #['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']   
    if interval == '1m': 
        return 60
    elif interval == '3m':
            return 60 * 3  
    elif interval == '5m':
            return 60 * 5    
    elif interval == '15m':
            return 60 * 15    
    elif interval == '30m':
            return 60 * 30    
    elif interval == '1h':
            return 60 * 60    
    elif interval == '2h':
            return 60 * 60 * 2  
    elif interval == '4h':
            return 60 * 60 * 4    
    elif interval == '6h':
            return 60 * 60 * 6        
    elif interval == '8h':
            return 60 * 60 * 8        
    elif interval == '12h':
            return 60 * 60 * 12        
    elif interval == '1d':
            return 60 * 60 * 24        
    elif interval == '3d':
            return 60 * 60 * 24 * 3      
    elif interval == '1w':
            return 60 * 60 * 24 * 7          
    elif interval == '1M':
            return 60 * 60 * 24 * 30 

    if interval.__contains__('h'):
        return int(double(interval[:len(interval)-1]) * 60 * 60)  
    if interval.__contains__('m'):
        return int(double(interval[:len(interval)-1]) * 60)
        
def time_to_interval(time:int):
    #['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']   
    if time == 60:
        return '1m'
    elif time == 180:
        return '3m' 
    elif time == 300:
        return '5m'
    elif time == 900:
        return '15m'    
    elif time == 1800:
        return '30m'    
    elif time == 3600:
        return '1h'    
    elif time == 7200:
        return '2h'  
    elif time == 14400:
        return '4h'    
    elif time == 21600:
        return '6h'       
    elif time == 28800:
        return '8h'        
    elif time == 43200:
        return '12h'        
    elif time == 86400:
        return '1d'        
    elif time == 259200:
        return '3d'      
    elif time == 604800:
        return '1w'          
    elif time == 2592000:
        return '1M' 
    pass

def Get_Klines(client, currencies):
    print('Getting klines...')
    result={}
    for currency in currencies:
        kline={}
        while True:
            if is_api_call_possible(KLINES_100_500_WEIGHT):
                t = time.time() * 1000
                Repository.add_weight(weight=KLINES_100_500_WEIGHT, method='klines_{0}'.format(config['bar_count']), time=t)
                kline = {currency:client.klines(currency, config['kline_interval'], limit=config['bar_count'])}
                Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
                break
                #wait 1 second and try to repeat
            time.sleep(1)

        if len(kline[currency]) == config['bar_count']:
            result.update(kline)
        else:
            raise Exception('Not enough bars to extract: {0}/{1}'.format(len(kline[currency]), config['bar_count']))
    return result

def update_currencies():


    while True:
        if is_api_call_possible(LEVERAGE_INFO_WEIGHT+EXCHANGE_INFO_WEIGHT):
            t = time.time() * 1000
            Repository.add_weight(weight=EXCHANGE_INFO_WEIGHT, method='exchange_info', time=t)
            prices = client.exchange_info()['symbols']
            currencies = [c[0] for c in Repository.get_all_currencies()]
            currencies_to_update = [(p['pricePrecision'], p['quantityPrecision'], p['filters'][5]['notional'], p['filters'][0]['tickSize'], p['symbol']) for p in prices if currencies.__contains__(p['symbol'])]
            Repository.update_currencies(currencies_to_update)

            #values.append( (p['symbol'],p['pricePrecision'], p['quantityPrecision'], p['filters'][5]['notional'], p['filters'][0]['tickSize']))
            #Repository.seed_currencies(values)
            Repository.add_weight(weight=LEVERAGE_INFO_WEIGHT, method='leverage_brackets', time=t)
            leverages = {entry['symbol']:20 if max([b['initialLeverage'] for b in entry['brackets']]) >= 20 else max([b['initialLeverage'] for b in entry['brackets']]) for entry in client.leverage_brackets() if currencies.__contains__(entry['symbol']) }
            leverages_to_update = [(leverages[currency], currency) for currency in currencies]
            Repository.update_leverages(leverages_to_update)

            Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
            break
        #wait 1 second and try to repeat
        time.sleep(1)

def seed_currencies():


    while True:
        if is_api_call_possible(LEVERAGE_INFO_WEIGHT+EXCHANGE_INFO_WEIGHT):
            t = time.time() * 1000
            Repository.add_weight(weight=EXCHANGE_INFO_WEIGHT, method='exchange_info', time=t)
            prices = client.exchange_info()['symbols']
            currencies = []
            with open('cryptolist.txt', 'r') as file:
                currencies = [c.strip() for c in file.readlines()]
            #currencies_to_update = [(p['pricePrecision'], p['quantityPrecision'], p['filters'][5]['notional'], p['filters'][0]['tickSize'], p['symbol']) for p in prices if currencies.__contains__(p['symbol'])]

            values =  [(p['symbol'],p['pricePrecision'], p['quantityPrecision'], p['filters'][5]['notional'], p['filters'][0]['tickSize']) for p in prices if currencies.__contains__(p['symbol'])]
            Repository.seed_currencies(values)
            Repository.add_weight(weight=LEVERAGE_INFO_WEIGHT, method='leverage_brackets', time=t)
            leverages = {entry['symbol']:20 if max([b['initialLeverage'] for b in entry['brackets']]) >= 20 else max([b['initialLeverage'] for b in entry['brackets']]) for entry in client.leverage_brackets() if currencies.__contains__(entry['symbol']) }
            leverages_to_update = [(leverages[currency], currency) for currency in currencies]
            Repository.update_leverages(leverages_to_update)

            Repository.update_weight_time(old_time=t, new_time=time.time() * 1000)
            break
        #wait 1 second and try to repeat
        time.sleep(1)


if __name__ == '__main__':
    #update_currencies() 
    BacktestRepository.Execute("DELETE FROM klines_1m;")
    seed_backtest_klines()
    run_backtest()  
    manage_backtest_cointegrations()
    #check_cointgrations()
    
    
    #Repository.seed_database(ONLINE_TEST_MODE)
    


    #trade_calculator_loop()
    #Repository.Execute("DELETE FROM pair_combinations")
    #coins = [c[0] for c in Repository.get_all_currencies()]
    #comb = [[f'{s1}/{s2}'] for s1, s2 in list(combinations(coins, 2))]
    #Repository.ExecuteMany("INSERT INTO pair_combinations(pair) VALUES(?)", comb)
    pass

    #p1 = Process(target=check_cointgrations)
    #p2 = Process(target=pair_loop)
    #p3 = Process(target=trade_calculator_loop)
    #p1.start()
    #p2.start()
    #p3.start()
    #p1.join()
    #p2.join()
    #p3.join()


pass









