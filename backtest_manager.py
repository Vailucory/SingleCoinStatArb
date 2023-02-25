from DataBase import Repository, BacktestRepository, orders_definition 
from interval_converter import IntervalConverter
from config_manager import ConfigManager
import time
from math_helper import MathHelper
from multiprocessing import Process
from Bot import close_market_order, manage_market_order_creation, ORDER_CREATION_AVAILABLE_BALANCE_TRESHOLD, PUMP_DUMP_INTERCEPT_MODE
from Binance import BinanceAPI
from datetime import datetime
from hurst import compute_Hc

def seed_backtest_klines():

    config = ConfigManager.config
    client = BinanceAPI.client


    


    bar_count = config['bar_count']
    interval = IntervalConverter.interval_to_time(config['kline_interval'])
    t = bar_count * interval

    #make smart klines loading
    start_date = datetime(year=2022, month=9, day=27, hour=10)
    end_date = datetime(year=2022, month=12, day=27, hour=10)
    
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
        start_time_1h = start_time - config['bar_count'] * 3600000
        end_time_1h = end_time
        i += 1

        print('Loading {0} | {1} from {2}'.format(symbol, i, len(currencies)))
        klines_1h = []
        while True:
            kline = [k for k in BinanceAPI.get_kline(kline_interval='1h', limit=limit, symbol=symbol, start_time=start_time_1h) if k[0]< end_time_1h]
            klines_1h.extend(kline)
            if len(kline) != 0:
                start_time_1h = int(kline[-1][0])+1
            if len(kline) < limit:
                break  
        print('Loaded 1h for {0} | Time elapsed: {1}'.format(symbol, time.time()-log_time_start))
        #tmp = klines_1h[-1]
        BacktestRepository.add_klines(symbol,klines_1h,'1h')
        print(len(klines_1h))

        start_time = int(datetime.timestamp(start_date)*1000)
        start_time_1m = start_time# + 24 * 3600000#- config['bar_count'] * 3600000
        end_time_1m = end_time + 24 * 3600000

        klines_1m = []
        while True:
            kline = [k for k in BinanceAPI.get_kline(kline_interval='1m', limit=limit, symbol=symbol, start_time=start_time_1m) if k[0]< end_time_1m]
            klines_1m.extend(kline)
            if len(kline) != 0:
                start_time_1m = int(kline[-1][0])+1
            if len(kline) < limit:
                break  
        print('Loaded 1m for {0} | Time elapsed: {1}'.format(symbol, time.time()-log_time_start))
        #tmp = klines_1m[-1]
        BacktestRepository.add_klines(symbol,klines_1m,'1m')
        print(len(klines_1m))
 

    pass


def calculate_symbol_preloaded_data(symbol:str, bars, last_counted_time:int=None):
    result = []
    bar_count = ConfigManager.config['bar_count']
    #bar_count = 240
    bars_closes = [b[5] for b in bars]
    bars_times = [b[6] for b in bars]

    for i in range(len(bars)-bar_count):
        if last_counted_time is not None and bars[i+bar_count][6] <= last_counted_time:
            continue
        closes_slice = bars_closes[i:i+bar_count]
        deviation = MathHelper.calculate_deviation(closes_slice)
        coefs = MathHelper.calculate_linear_regression_coefficients(bars_times[i:i+bar_count], closes_slice)
        hurst_exp = compute_Hc(closes_slice, kind='price', simplified=True)[0]
        result.append((bars[i+bar_count][6], deviation, coefs[0], coefs[1], hurst_exp, symbol))
        pass
    BacktestRepository.add_symbols_preloaded_data(result)    
 

def calculate_symbols_preloaded_data(*args):
    for symbol in args: 
        bars = BacktestRepository.get_backtest_klines1h_by_symbol(symbol)
        calculate_symbol_preloaded_data(symbol, bars)

def manage_symbols_preloaded_data():
    split_count = 3
    preloaded_data_count = 0
    pass
    symbols = list(split([p[0] for p in BacktestRepository.ExecuteWithResult(f"SELECT symbol FROM currencies")],split_count))

    processes = []
    for i in range(split_count):
        processes.append(Process(target=calculate_symbols_preloaded_data, args=list(symbols[i])))
    for p in processes:
        p.start()
    for p in processes:
        p.join()    

 
def process_backtest_slice(start_time:int, end_time, preloaded_symbols_info, coins, klines_1h, is_trade_creation_allowed=True):
    if is_trade_creation_allowed:
        add_update_backtest_symbols(preloaded_symbols_info)
    klines_1m = BacktestRepository.get_backtest_klines1m(start_time, end_time)
    keys = list(klines_1m.keys())
    symbol_index = 0
    high = 1
    low = 2
    close = 3
    for close_time in keys:
        klines = klines_1m[close_time]

        #check active orders for completion
        coins_with_active_orders = BacktestRepository.get_coins_with_open_orders()

        klines_with_active_orders = {}
        [klines_with_active_orders.update({k[symbol_index]:k}) for k in klines if coins_with_active_orders.__contains__(k[symbol_index])]
        
        for order in BacktestRepository.get_all_orders():
            current_price = klines_with_active_orders[order[orders_definition.symbol]][close]
            current_profit = order[orders_definition.currentProfit] + MathHelper.calculate_order_profit(enter_price=order[orders_definition.lastPrice],
                                                                                                triggered_price=current_price,
                                                                                                quantity=order[orders_definition.origQty],
                                                                                                order_type=order[orders_definition.side])
            BacktestRepository.add_to_available_balance(current_profit-order[orders_definition.currentProfit], close_time)
            BacktestRepository.update_order_current_parameters(current_profit=current_profit,
                                                                last_price=current_price,
                                                                symbol=order[orders_definition.symbol])
    
        is_program_shutdown_started = BacktestRepository.get_is_program_shutdown_started()
        #for coin1, coin2 in BacktestRepository.get_coins_with_open_orders_by_hedges():
        for symbol_info in BacktestRepository.get_coins_with_open_orders():

            market = BacktestRepository.get_active_order_by_type(symbol_info, 'MARKET') 
            deviation, lin_reg_coef_a, lin_reg_coef_b = BacktestRepository.get_symbol_info(market[orders_definition.symbol])
            current_price = klines_with_active_orders[market[orders_definition.symbol]][close]

            if close_time - market[orders_definition.updateTime] > IntervalConverter.interval_to_time(ConfigManager.config['order_stop_limit_time']) * 1000:
                #close trades via market and exit
                close_market_order(market, current_price, close_time, 'TIME_STOP')
                continue
            
            #calculate bounds
            coefficients_linear_regression = (lin_reg_coef_a, lin_reg_coef_b)
            linear_regression_bound = MathHelper.calculate_polynom(close_time, coefficients_linear_regression)
            price_stop_percentage = BacktestRepository.get_price_stop_percentage()
            market_exceeded_limits = MathHelper.is_price_exceeded_limit(market[orders_definition.price], current_price, market[orders_definition.leverage], price_stop_percentage, market[orders_definition.side])
            
            if is_program_shutdown_started:
                close_market_order(market, current_price, close_time, 'PROGRAM_CLOSURE')  
            elif market_exceeded_limits[0]:
                close_market_order(market, current_price, close_time, 'PRICE_STOP_LIMIT_EXCEEDED')
                BacktestRepository.log_message(f"Symbol {market[orders_definition.symbol]} exceeded price stop limit with: {market_exceeded_limits[1]}. TradeId: {market[orders_definition.tradeId]}")
            #up: short(sell)
            #down: long(buy)
            #we entered when crossed upper deviation bound
            elif market[orders_definition.side] == 'SELL':
                #close market order if current price is lower than linear regression bound
                if current_price <= linear_regression_bound:
                    close_market_order(market, current_price, close_time, 'LINEAR_REGRESSION_CROSSING')
            #we entered when crossed lower deviation bound
            else:
                #close market order if current price is higher than linear regression bound
                if current_price >= linear_regression_bound:
                    close_market_order(market, current_price, close_time, 'LINEAR_REGRESSION_CROSSING')
        
        if not is_trade_creation_allowed:
            continue
        
        active_symbols = BacktestRepository.get_active_symbols() 

        active_symbols_klines = {}
        [active_symbols_klines.update({k[symbol_index]:k}) for k in klines if [symbol[0] for symbol in active_symbols].__contains__(k[symbol_index])]
        for symbol_info in active_symbols:

            symbol = symbol_info[0]
            hurst_exponent = symbol_info[4]
            is_outside_deviations = bool(symbol_info[5]) 
            #exit if active order on coin
            if BacktestRepository.check_if_orders_available(symbol):
                continue

            if is_outside_deviations:
                deviation = symbol_info[1]
                coin_price = active_symbols_klines[symbol][close]
                coefficients_linear_regression = (symbol_info[2], symbol_info[3])
                deviation_multiplier = ConfigManager.config['lin_reg_deviation']
                epoch_time = close_time
                linear_regression_bound = MathHelper.calculate_polynom(epoch_time, coefficients_linear_regression)
                balance = BacktestRepository.get_balance()
                if BacktestRepository.get_available_balance() < balance * ORDER_CREATION_AVAILABLE_BALANCE_TRESHOLD:
                    continue
                if PUMP_DUMP_INTERCEPT_MODE:
                    last_price = BacktestRepository.get_symbol_last_price(symbol)
                    deviation_coefficient = deviation_multiplier * 0.4
                    if coin_price > linear_regression_bound and last_price - coin_price >= deviation * deviation_coefficient and coin_price - linear_regression_bound >= deviation * deviation_coefficient:
                        currency_info = BacktestRepository.get_currency(symbol)
                        max_notional = 1000000

                        leverage = currency_info[5]
                        side = 'SELL'
                        quantity = MathHelper.calculate_quantity(total=balance,
                                                                entry_price=coin_price,
                                                                precision=currency_info[2],
                                                                minimum_notion=currency_info[3],
                                                                maximum_notion=max_notional,
                                                                leverage=leverage)
                        
                        manage_market_order_creation(coin=symbol, 
                                                    quantity=quantity, 
                                                    current_price=coin_price, 
                                                    side=side,
                                                    stop_price=None,
                                                    leverage=leverage, 
                                                    backtest_time=close_time) 
                        BacktestRepository.update_symbol_is_outside_deviation(symbol=symbol, is_outside_deviation=0, time=close_time)   
                    elif coin_price < linear_regression_bound and coin_price - last_price >= deviation * deviation_coefficient and linear_regression_bound - coin_price >= deviation * deviation_coefficient:
                        currency_info = BacktestRepository.get_currency(symbol)
                        max_notional = 1000000

                        leverage = currency_info[5]
                        side = 'BUY'
                        quantity = MathHelper.calculate_quantity(total=balance,
                                                                entry_price=coin_price,
                                                                precision=currency_info[2],
                                                                minimum_notion=currency_info[3],
                                                                maximum_notion=max_notional,
                                                                leverage=leverage)
                        
                        manage_market_order_creation(coin=symbol, 
                                                    quantity=quantity, 
                                                    current_price=coin_price, 
                                                    side=side,
                                                    stop_price=None,
                                                    leverage=leverage, 
                                                    backtest_time=close_time) 
                        BacktestRepository.update_symbol_is_outside_deviation(symbol=symbol, is_outside_deviation=0, time=close_time)


                    if close_time - BacktestRepository.get_symbol_iod_last_updated(symbol) <= 3600000:
                        BacktestRepository.update_symbol_is_outside_deviation(symbol=symbol, is_outside_deviation=0, time=close_time)    
                    if (coin_price > linear_regression_bound and coin_price - linear_regression_bound < deviation * deviation_coefficient) or (coin_price < linear_regression_bound and linear_regression_bound - coin_price < deviation * deviation_coefficient):    
                        BacktestRepository.update_symbol_is_outside_deviation(symbol=symbol, is_outside_deviation=0, time=close_time)
                else:
                    #getting linear regression polynom coefficients and deviation value
                    deviation_multiplier = ConfigManager.config['lin_reg_deviation']
                    
                    
                    coefficients_up = (symbol_info[2], symbol_info[3] + (deviation * deviation_multiplier))
                    coefficients_down = (symbol_info[2], symbol_info[3] - (deviation * deviation_multiplier))
                    
                    upper_bound = MathHelper.calculate_polynom(epoch_time, coefficients_up)
                    lower_bound = MathHelper.calculate_polynom(epoch_time, coefficients_down)
                    
                    #calculating pair relation at the moment
                    

                    if coin_price < upper_bound and coin_price > lower_bound:
                        
                        #skip if price was outside deviation less than hour to prevent early order creation
                        if close_time - BacktestRepository.get_symbol_iod_last_updated(symbol) <= 3600000:
                            BacktestRepository.update_symbol_is_outside_deviation(symbol=symbol, is_outside_deviation=0, time=close_time)
                            continue

                        BacktestRepository.update_symbol_is_outside_deviation(symbol=symbol, is_outside_deviation=0, time=close_time)
                        
                        balance = BacktestRepository.get_balance()
                        if BacktestRepository.get_available_balance() < balance * ORDER_CREATION_AVAILABLE_BALANCE_TRESHOLD:
                            continue
                        
                        if not BacktestRepository.get_is_order_creation_allowed():
                            continue

                        currency_info = BacktestRepository.get_currency(symbol)
                        max_notional = 1000000

                        leverage = currency_info[5]
                        side = 'SELL' if coin_price > linear_regression_bound else 'BUY'

                        quantity = MathHelper.calculate_quantity(total=balance,
                                                                entry_price=coin_price,
                                                                precision=currency_info[2],
                                                                minimum_notion=currency_info[3],
                                                                maximum_notion=max_notional,
                                                                leverage=leverage)
                        
                        manage_market_order_creation(coin=symbol, 
                                                    quantity=quantity, 
                                                    current_price=coin_price, 
                                                    side=side,
                                                    stop_price=None,
                                                    leverage=leverage, 
                                                    backtest_time=close_time)  
                        
                        continue
                    pass
            
    
            #exit if not anti-persistent series
            if hurst_exponent >= 0.5 and not PUMP_DUMP_INTERCEPT_MODE:
                continue

            deviation_multiplier = ConfigManager.config['lin_reg_deviation']
            deviation = symbol_info[1]
            coefficients_up = (symbol_info[2], symbol_info[3] + (deviation * deviation_multiplier))
            coefficients_down = (symbol_info[2], symbol_info[3] - (deviation * deviation_multiplier))
            epoch_time = close_time
            upper_bound = MathHelper.calculate_polynom(epoch_time, coefficients_up)
            lower_bound = MathHelper.calculate_polynom(epoch_time, coefficients_down)


            coin_price = active_symbols_klines[symbol][close]

            if coin_price > upper_bound or coin_price < lower_bound:
                BacktestRepository.update_symbol_is_outside_deviation(symbol=symbol, is_outside_deviation=1, time=close_time)     
                if PUMP_DUMP_INTERCEPT_MODE:
                    BacktestRepository.update_symbol_last_price(symbol, coin_price)   
        pass
        


    pass

def run_backtest():
    BacktestRepository.seed_database()
    close_times = BacktestRepository.get_backtest_klines1h_times()
    bar_count = ConfigManager.config['bar_count']
    interval = IntervalConverter.interval_to_time(ConfigManager.config['kline_interval']) * 1000
    coins = list([c[0] for c in BacktestRepository.get_all_currencies()])
    #168-912
    start_index = bar_count
    end_index = len(close_times)
    for i in range(start_index, end_index):
        start_logging_time = time.time()
        t0 = close_times[i - bar_count]
        t1 = close_times[i]
        t2 = t1 - interval

        preloaded_symbols_info = BacktestRepository.get_preloaded_symbols_info(time=t1)
        klines_1h = BacktestRepository.get_backtest_klines1h(start_time=t0, end_time=t1) 
        process_backtest_slice(t2,t1, preloaded_symbols_info, coins, klines_1h)

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

def add_update_backtest_symbols(symbols):
    if len(BacktestRepository.get_active_symbols()) == 0:
        BacktestRepository.add_active_symbols(symbols)
        return
    BacktestRepository.update_symbols(symbols)

def split(a:list, n:int):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))


if __name__ == '__main__':
    #manage_symbols_preloaded_data()
    run_backtest()
    pass