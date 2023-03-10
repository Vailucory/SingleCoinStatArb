import sqlite3
import time
from config_manager import ConfigManager
from numpy import double
import mysql.connector
from interval_converter import IntervalConverter

def static_init(cls):
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls

@static_init
class orders_definition:
    orderId = 0
    symbol = 1
    status = 2
    clientOrderId = 3
    price = 4
    avgPrice = 5
    origQty = 6
    executedQty = 7
    cumQuote = 8
    timeInForce = 9 
    type = 10
    reduceOnly = 11
    closePosition = 12
    side = 13
    positionSide = 14
    stopPrice = 15
    workingType = 16
    priceProtect = 17
    origType = 18
    updateTime = 19
    tradeId = 20
    leverage = 21
    lastPrice =	22
    currentProfit = 23

@static_init
class BacktestRepository:
    db = sqlite3.connect(ConfigManager.config['backtest_connection_string'])

    def __del__(self):
        BacktestRepository.db.close()
        pass
    @staticmethod
    def Execute(command:str, params=None):
        while True:
            try:
                command=command.replace('_interval', 'interval')
                c = BacktestRepository.db.cursor()
                if params is None:
                    c.execute(command)
                else:
                    c.execute(command, params)
                BacktestRepository.db.commit()
                break
            except sqlite3.OperationalError as err:
                if str(err).lower().count('locked') > 1:
                    continue
                else:
                    BacktestRepository.log_message(command + " | " +  str(err))   
                    raise err
    @staticmethod
    def ExecuteMany(command, params):
        while True:
            try:
                command=command.replace('_interval', 'interval')
                c = BacktestRepository.db.cursor()
                c.executemany(command, params)
                BacktestRepository.db.commit()
                break
            except sqlite3.OperationalError as err:
                if str(err).lower().count('locked') > 1:
                    continue
                else:
                    BacktestRepository.log_message(command + " | " +  str(err))   
                    raise err 
    @staticmethod
    def ExecuteWithResult(command, params=None):
        while True:
            try:  
                command=command.replace('_interval', 'interval')  
                c = BacktestRepository.db.cursor()
                if params is None:
                    c.execute(command)
                else:
                    c.execute(command, params)
                BacktestRepository.db.commit()
                return c.fetchall()
            except sqlite3.OperationalError as err:
                if str(err).lower().count('locked') > 1:
                    continue
                else:
                    BacktestRepository.log_message(command + " | " +  str(err))   
                    raise err     
    
    @staticmethod
    def get_balance_history():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM balance_history")
    @staticmethod
    def get_available_balance_history():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM available_balance_history")
    @staticmethod
    def get_all_currencies():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM currencies")

    @staticmethod
    def add_symbols_preloaded_data(values):
        BacktestRepository.ExecuteMany("INSERT INTO preloaded_symbols_info(time,deviation,lin_reg_coef_a,lin_reg_coef_b,hurst_exponent,symbol) VALUES(?,?,?,?,?,?)", values)

    @staticmethod
    def update_backtest_cointegrations(values):
        BacktestRepository.ExecuteMany("UPDATE cointegrations SET deviation=?, lin_reg_coef_a=?, lin_reg_coef_b=? WHERE time==? AND pairId==?", values)

    @staticmethod
    def get_backtest_klines1h_by_symbol(symbol):
        return BacktestRepository.ExecuteWithResult(f"SELECT * FROM klines_1h WHERE symbol == '{symbol}' ORDER BY close_time")
    @staticmethod
    def get_backtest_klines1m_by_symbol(symbol):
        return BacktestRepository.ExecuteWithResult(f"SELECT * FROM klines_1m WHERE symbol == '{symbol}' ORDER BY close_time")

    @staticmethod
    def get_backtest_klines1m(start_time:int, end_time:int):
        querry_result = BacktestRepository.ExecuteWithResult(f"""SELECT symbol, high, low, close, close_time 
                                                                FROM klines_1m 
                                                                WHERE close_time > {start_time} AND close_time <= {end_time} 
                                                                ORDER BY symbol, close_time""")
        close_times = list([k[4] for k in querry_result[:60]])
        result = {}
        for close_time in close_times:
            result.update({close_time:[(kline[0],kline[1],kline[2],kline[3]) for kline in querry_result if kline[4] == close_time]})
            pass
        pass
        return result

    @staticmethod
    def get_backtest_pairs():
        return list([f[0] for f in BacktestRepository.ExecuteWithResult("SELECT pair FROM pairs")])
    @staticmethod
    def get_backtest_uncalculated_pairs():
        querry_result = BacktestRepository.ExecuteWithResult("""SELECT pair FROM pairs 
                                                                LEFT JOIN cointegrations ON pairs.id = cointegrations.pairId
                                                                WHERE cointegrations.time IS null""")
        return [f[0] for f in querry_result]

    @staticmethod
    def add_klines(symbol, klines, interval):
        int_interval = IntervalConverter.interval_to_time(interval)
        result = [(symbol, k[0], k[1], k[2], k[3], k[4], k[6], int_interval) for k in klines]
        BacktestRepository.ExecuteMany(f"INSERT INTO klines_{interval}(symbol,open_time,open,high,low,close,close_time,_interval) VALUES(?,?,?,?,?,?,?,?)", result)

    pass
    @staticmethod
    def get_balance():
        return double(BacktestRepository.get_variable('balance'))
    @staticmethod    
    def set_balance(value, time = int(time.time()*1000)):
        BacktestRepository.update_variable('balance', value)
        BacktestRepository.add_balance_snapshot(value, time)
    @staticmethod
    def add_to_balance(value, time = int(time.time()*1000)):
        BacktestRepository.Execute(f"UPDATE variables SET value=value+{value} WHERE name == 'balance'")
        BacktestRepository.add_balance_snapshot(BacktestRepository.get_balance(), time)
    @staticmethod
    def add_balance_snapshot(balance, time):
        BacktestRepository.Execute("INSERT INTO balance_history(balance, change_time) VALUES(?,?)", (balance, time))
    @staticmethod
    def get_available_balance():
        return double(BacktestRepository.get_variable('available_balance'))
    @staticmethod    
    def set_available_balance(value, time):
        BacktestRepository.update_variable('available_balance', value)
        BacktestRepository.add_available_balance_snapshot(value, time)      
    @staticmethod    
    def add_to_available_balance(value, time):
        BacktestRepository.Execute("UPDATE variables SET value=value+? WHERE name == 'available_balance'", (value,))
        BacktestRepository.add_available_balance_snapshot(BacktestRepository.get_available_balance(), time)  
    @staticmethod
    def add_available_balance_snapshot(balance, time = int(time.time()*1000)):
        BacktestRepository.Execute("INSERT INTO available_balance_history(balance,change_time) VALUES(?,?)", (balance, time))
    @staticmethod
    def get_is_order_creation_allowed():
        return int(BacktestRepository.get_variable('is_order_creation_allowed'))
    @staticmethod    
    def set_is_order_creation_allowed(value):
        BacktestRepository.update_variable('is_order_creation_allowed', value)    
    @staticmethod
    def get_price_stop_percentage():
        return BacktestRepository.get_variable('price_stop_percentage')
    @staticmethod
    def get_is_program_shutdown_started():
        return bool(BacktestRepository.get_variable('is_program_shutdown_started'))
    @staticmethod    
    def set_is_program_shutdown_started(value):
        BacktestRepository.update_variable('is_program_shutdown_started', value)  
    @staticmethod
    def get_variable(name):
        return BacktestRepository.ExecuteWithResult(f"SELECT value FROM variables WHERE name == '{name}'")[0][0]
    @staticmethod
    def update_variable(name, value):
        BacktestRepository.Execute(f"UPDATE variables SET value = {value} WHERE name == '{name}'")
    @staticmethod
    def update_symbol_last_price(symbol, last_price):
        BacktestRepository.Execute("UPDATE symbols SET last_price=? WHERE symbol == ?", (last_price, symbol))
    @staticmethod
    def get_symbol_last_price(symbol):
        return BacktestRepository.ExecuteWithResult("SELECT last_price FROM symbols WHERE symbol == ?", (symbol,))[0][0]


    @staticmethod
    def seed_database():
        BacktestRepository.Execute("DELETE FROM logs")  
        BacktestRepository.Execute("DELETE FROM orders")    
        BacktestRepository.Execute("DELETE FROM symbols")    
        BacktestRepository.Execute("DELETE FROM backtest_results")
        BacktestRepository.Execute("DELETE FROM orders_archive")  
        BacktestRepository.Execute("DELETE FROM balance_history")   
        BacktestRepository.Execute("DELETE FROM available_balance_history")  
        #BacktestRepository.Execute("INSERT OR IGNORE INTO variables VALUES('is_program_shutdown_started', 0)") 
        BacktestRepository.set_available_balance(30, 0)
        BacktestRepository.set_balance(30, 0)
        BacktestRepository.set_is_order_creation_allowed(True)
        BacktestRepository.set_is_program_shutdown_started(False)


    @staticmethod
    def get_preloaded_symbols_info(time:int):
        querry_result = BacktestRepository.ExecuteWithResult(f"""SELECT symbol, deviation, lin_reg_coef_a, lin_reg_coef_b, hurst_exponent, time 
                                                                FROM preloaded_symbols_info 
                                                                WHERE preloaded_symbols_info.time == {time}""")
        return querry_result


    @staticmethod
    def get_active_pairs_symbols():
        querry_result = BacktestRepository.ExecuteWithResult("SELECT pair FROM active_pairs")
        return list([r[0] for r in querry_result])

    @staticmethod
    def add_active_symbols(symbols:list):
        symbols_to_import = []
        #pair, adf, eg, deviation, lin_reg_coef_a, lin_reg_coef_b, time
        for symbol in symbols:
            symbols_to_import.append((symbol[0],   #symbol
                                    0,         #is outside deviation
                                    symbol[1],   #deviation
                                    symbol[2],   #lin_reg_coef_a
                                    symbol[3],   #lin_reg_coef_b
                                    symbol[4],   #hurst_exponent
                                    symbol[5]))  #time
        BacktestRepository.ExecuteMany("INSERT OR IGNORE INTO symbols(symbol,is_outside_deviation,deviation,lin_reg_coef_a,lin_reg_coef_b,hurst_exponent,last_updated) VALUES(?,?,?,?,?,?,?)", symbols_to_import)


    @staticmethod
    def update_symbols(symbols:list):
        symbols_to_import = []
        #pair, adf, eg, deviation, lin_reg_coef_a, lin_reg_coef_b, time
        for symbol in symbols:
            symbols_to_import.append((
                                    symbol[1],  #deviation
                                    symbol[2],  #a
                                    symbol[3],  #b
                                    symbol[4],  #hurst_exponent
                                    symbol[5],  #time
                                    symbol[0])) #pair
        BacktestRepository.ExecuteMany("""
                        UPDATE symbols 
                        SET deviation=?, lin_reg_coef_a=?, lin_reg_coef_b=?, hurst_exponent=?, last_updated=?
                        WHERE symbol == ?""", symbols_to_import)

    @staticmethod
    def get_coins_with_open_orders():
        return [o[0] for o in BacktestRepository.ExecuteWithResult("SELECT DISTINCT symbol FROM orders")]  
    @staticmethod
    def get_active_order_by_type(symbol, type):
        return BacktestRepository.ExecuteWithResult(f"SELECT * FROM orders WHERE symbol == '{symbol}' AND type LIKE '%{type}%'")[0]
    @staticmethod
    def get_active_min_rev_orders(symbol):
        querry_result = BacktestRepository.ExecuteWithResult(f"SELECT hedgeId FROM orders WHERE symbol == '{symbol}'")
        if len(querry_result) == 0:
            return None
        hedgeId = querry_result[0][0]
        return BacktestRepository.ExecuteWithResult(f"SELECT * FROM orders WHERE hedgeId == {hedgeId} AND type == 'MARKET'")
    @staticmethod
    def add_backtest_result(symbol, triggered_order_type, profit, enter_price, exit_price, quantity, enter_time, exit_time, tradeId):
        BacktestRepository.Execute("INSERT INTO backtest_results(symbol,triggered_order_type,profit,enter_price, exit_price, quantity,enter_time,exit_time,tradeId) VALUES(?,?,?,?,?,?,?,?,?)",
                                    (symbol, triggered_order_type, profit, enter_price, exit_price, quantity, enter_time, exit_time, tradeId) )

    @staticmethod
    def remove_orders(symbol):
        BacktestRepository.Execute(f"DELETE FROM orders WHERE symbol == '{symbol}'")

    @staticmethod
    def archive_order(order):
        BacktestRepository.Execute("INSERT INTO orders_archive(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,leverage) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", order)

    @staticmethod
    def get_active_symbols():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM symbols")


    @staticmethod
    def check_if_orders_available(symbol):
        querry_result = BacktestRepository.ExecuteWithResult(f"SELECT * FROM orders WHERE symbol == '{symbol.upper()}'")
        return len(querry_result) > 0    
    @staticmethod
    def update_symbol_is_outside_deviation(symbol, is_outside_deviation, time=int(time.time()*1000)):
        BacktestRepository.Execute(f"UPDATE symbols SET is_outside_deviation = {is_outside_deviation}, iod_last_updated = {time} WHERE symbol == '{symbol}'")   
    @staticmethod
    def get_symbol_iod_last_updated(symbol): 
        return BacktestRepository.ExecuteWithResult(f"SELECT iod_last_updated FROM symbols WHERE symbol == '{symbol}'")[0][0]
    @staticmethod
    def get_currency(symbol):
        return BacktestRepository.ExecuteWithResult(f"SELECT * FROM currencies WHERE symbol == '{symbol}'")[0]
    @staticmethod
    def log_message(message):
        BacktestRepository.Execute("INSERT INTO logs(time,message) VALUES(?,?)",(int(time.time()*1000), message))    
   
    @staticmethod
    def get_backtest_klines1h(start_time:int, end_time:int):
        querry_result = BacktestRepository.ExecuteWithResult(f"""SELECT symbol, close 
                                                                FROM klines_1h 
                                                                WHERE close_time >= {start_time} AND close_time < {end_time} 
                                                                ORDER BY close_time, symbol""")
                                                           
        symbols_count = int(BacktestRepository.ExecuteWithResult("SELECT COUNT(*) FROM currencies")[0][0])
        symbols = list([k[0] for k in querry_result[:symbols_count]])
        result = {}
        for symbol in symbols:
            result.update({symbol:[kline[1] for kline in querry_result if kline[0] == symbol]})
            pass
        pass
        return result   
    @staticmethod
    def get_backtest_klines1h_times():
        return list([r[0] for r in BacktestRepository.ExecuteWithResult("SELECT DISTINCT close_time FROM klines_1h ORDER BY close_time")])

    @staticmethod
    def add_order(order):
        BacktestRepository.Execute("""INSERT INTO orders(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,leverage,lastPrice,currentProfit)
                                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (order['orderId'], 
                                                         order['symbol'], 
                                                         order['status'], 
                                                         order['clientOrderId'], 
                                                         order['price'], 
                                                         order['avgPrice'], 
                                                         order['origQty'], 
                                                         order['executedQty'], 
                                                         order['cumQuote'], 
                                                         order['timeInForce'], 
                                                         order['type'], 
                                                         order['reduceOnly'], 
                                                         order['closePosition'], 
                                                         order['side'], 
                                                         order['positionSide'], 
                                                         order['stopPrice'], 
                                                         order['workingType'], 
                                                         order['priceProtect'], 
                                                         order['origType'], 
                                                         order['updateTime'],
                                                         order['tradeId'],
                                                         order['leverage'],
                                                         order['lastPrice'],
                                                         order['currentProfit']))


    @staticmethod
    def remove_coin_from_backtest(coins):

        if len(coins) == 0:
            return

        delete_cointegrations_querry = f"""DELETE FROM cointegrations WHERE pairId IN (SELECT id FROM pairs WHERE pair LIKE {" OR pair LIKE ".join([f"'%{coin}%'" for coin in coins])});"""
        delete_pairs_querry = f"""DELETE FROM pairs WHERE pair LIKE {" OR pair LIKE ".join([f"'%{coin}%'" for coin in coins])};"""
        delete_klines_1h_querry = f"""DELETE FROM klines_1h WHERE symbol IN ({",".join([f"'{coin}'" for coin in coins])});"""
        delete_klines_1m_querry = f"""DELETE FROM klines_1m WHERE symbol IN ({",".join([f"'{coin}'" for coin in coins])});"""
        delete_currencies_querry = f"""DELETE FROM currencies WHERE symbol IN ({",".join([f"'{coin}'" for coin in coins])});"""
        query = f"""{delete_cointegrations_querry}
        {delete_pairs_querry}
        {delete_klines_1h_querry}
        {delete_klines_1m_querry}
        {delete_currencies_querry}"""   
        BacktestRepository.Execute(delete_cointegrations_querry)
        BacktestRepository.Execute(delete_pairs_querry)
        BacktestRepository.Execute(delete_klines_1h_querry)
        BacktestRepository.Execute(delete_klines_1m_querry)
        BacktestRepository.Execute(delete_currencies_querry)


    @staticmethod
    def get_max_klines_1m_time():
        return BacktestRepository.ExecuteWithResult("SELECT MAX(close_time) FROM klines_1m")[0][0]

    @staticmethod
    def get_symbol_info(symbol:str):
        return BacktestRepository.ExecuteWithResult(f"SELECT deviation, lin_reg_coef_a, lin_reg_coef_b FROM symbols WHERE symbol == '{symbol}'")[0]
    
    @staticmethod
    def get_coins_with_open_orders_by_hedges():
        hedges = [h[0] for h in BacktestRepository.ExecuteWithResult("SELECT DISTINCT hedgeId FROM orders")]
        if len(hedges) == 1 and hedges[0] is None:
            return []
        return [BacktestRepository.ExecuteWithResult("SELECT * FROM (SELECT substr(pair, 1, pos-1) AS coin1, substr(pair, pos+1) AS coin2 FROM (SELECT *, instr(pair, '/') AS pos FROM active_pairs)) WHERE coin1 IN (SELECT symbol FROM orders WHERE hedgeId == ?) AND coin2 IN (SELECT symbol FROM orders WHERE hedgeId == ?)", [hedge, hedge])[0] for hedge in hedges]
    @staticmethod
    def get_all_orders():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM orders")    

    @staticmethod
    def update_order_current_parameters(current_profit, last_price, symbol):
        BacktestRepository.Execute("UPDATE orders SET lastPrice = ?, currentProfit = ? WHERE symbol == ?", (last_price, current_profit, symbol))

    #variables are now REAL    
###########################################################################################################################################
###########################################################################################################################################



@static_init
class Repository:
    SQLITE = 'SQLITE'
    MYSQL = 'MYSQL'
    CURRENT_DATABASE = MYSQL
    db = sqlite3.connect(ConfigManager.config['connection_string']) if CURRENT_DATABASE == SQLITE else mysql.connector.connect(
        host="localhost",
        user=ConfigManager.config['mysql_login'],
        password=ConfigManager.config['mysql_password'],
        database=ConfigManager.config['mysql_database'],
        autocommit=True)

    def __del__(self):
        Repository.db.close()
        pass
    
    @staticmethod
    def Execute(command:str, params=None):
        while True:
            try:
                if Repository.CURRENT_DATABASE == Repository.SQLITE:
                    c = Repository.db.cursor()
                    if params is None:
                        c.execute(command)
                    else:
                        c.execute(command, params)
                    Repository.db.commit()
                    break
                elif Repository.CURRENT_DATABASE == Repository.MYSQL:
                    Repository.db.reconnect(3, 10)
                    c = Repository.db.cursor(prepared=True)
                    if params is None:
                        c.execute(Repository.validate_mysql_querry(command))
                    else:
                        c.execute(Repository.validate_mysql_querry(command), params)
                    Repository.db.commit()
                    break
                else:
                    raise NotImplementedError(f'Repository not implemented for {Repository.CURRENT_DATABASE}')
            except sqlite3.OperationalError as err:
                if str(err).lower().count('locked') > 1:
                    continue
                else:
                    Repository.log_message(command + " | " +  str(err))   
                    raise err
    @staticmethod
    def ExecuteMany(command, params):
        while True:
            try:
                if Repository.CURRENT_DATABASE == Repository.SQLITE:
                    c = Repository.db.cursor()
                    c.executemany(command, params)
                    Repository.db.commit()
                    break
                elif Repository.CURRENT_DATABASE == Repository.MYSQL:
                    Repository.db.reconnect(3, 10)
                    c = Repository.db.cursor(prepared=True)
                    c.executemany(Repository.validate_mysql_querry(command), params)
                    Repository.db.commit()
                    break
            except sqlite3.OperationalError as err:
                if str(err).lower().count('locked') > 1:
                    continue
                else:
                    Repository.log_message(command + " | " +  str(err))   
                    raise err 
    @staticmethod
    def ExecuteWithResult(command, params=None):
        while True:
            try:    
                if Repository.CURRENT_DATABASE == Repository.SQLITE:
                    c = Repository.db.cursor()
                    if params is None:
                        c.execute(command)
                    else:
                        c.execute(command, params)
                    Repository.db.commit()
                    return c.fetchall()
                elif Repository.CURRENT_DATABASE == Repository.MYSQL:
                    Repository.db.reconnect(3, 10)
                    c = Repository.db.cursor(prepared=True)
                    if params is None:
                        c.execute(Repository.validate_mysql_querry(command))
                    else:
                        c.execute(Repository.validate_mysql_querry(command), params)

                    return c.fetchall()    
            except sqlite3.OperationalError as err:
                if str(err).lower().count('locked') > 1:
                    continue
                else:
                    Repository.log_message(command + " | " +  str(err))   
                    raise err     

    @staticmethod
    def seed_database():
        Repository.Execute("DELETE FROM logs") 
        Repository.Execute("DELETE FROM symbol_information_updates")    
        Repository.Execute("DELETE FROM symbols") 
        Repository.Execute("DELETE FROM trades") 
        Repository.Execute("DELETE FROM available_balance_history") 
        Repository.Execute("DELETE FROM balance_history")     
        Repository.Execute("DELETE FROM orders")
        Repository.Execute("DELETE FROM orders_archive")  
        Repository.set_is_order_creation_allowed(True)
        Repository.set_is_program_shutdown_started(False)
        
    @staticmethod
    def get_symbol_last_price(symbol):
        return Repository.ExecuteWithResult("SELECT last_price FROM symbols WHERE symbol == ?", (symbol,))[0][0]
    @staticmethod
    def get_symbol_iod_last_updated(symbol): 
        return Repository.ExecuteWithResult(f"SELECT iod_last_updated FROM symbols WHERE symbol == '{symbol}'")[0][0]
    @staticmethod
    def update_symbol_last_price(symbol, last_price):
        Repository.Execute("UPDATE symbols SET last_price=? WHERE symbol == ?", (last_price, symbol))

    @staticmethod
    def get_available_balance():
        return double(Repository.get_variable('available_balance'))
    @staticmethod    
    def set_available_balance(value):
        Repository.update_variable('available_balance', value)
        Repository.add_available_balance_snapshot(value)    
    @staticmethod    
    def set_balance(value):
        Repository.update_variable('balance', value)
        Repository.add_balance_snapshot(value)        
    @staticmethod
    def get_balance():
        return double(Repository.get_variable('balance'))
    @staticmethod
    def add_to_balance(value):
        Repository.set_balance(Repository.get_balance()+value)

    @staticmethod
    def get_is_order_creation_allowed():
        return bool(Repository.get_variable('is_order_creation_allowed'))
    @staticmethod    
    def set_is_order_creation_allowed(value):
        Repository.update_variable('is_order_creation_allowed', value)    
    @staticmethod
    def get_price_stop_percentage():
        return Repository.get_variable('price_stop_percentage')
    @staticmethod
    def get_is_program_shutdown_started():
        return bool(Repository.get_variable('is_program_shutdown_started'))
    @staticmethod    
    def set_is_program_shutdown_started(value):
        Repository.update_variable('is_program_shutdown_started', value)  

    @staticmethod
    def get_variable(name):
        return Repository.ExecuteWithResult(f"SELECT value FROM variables WHERE name == '{name}'")[0][0]  
    @staticmethod
    def update_variable(name, value):
        Repository.Execute(f"UPDATE variables SET value = {value} WHERE name == '{name}'")
    @staticmethod
    def add_available_balance_snapshot(balance):
        Repository.Execute("INSERT INTO available_balance_history VALUES(?,?)", (balance, int(time.time()*1000)))
    @staticmethod
    def add_balance_snapshot(balance):
        Repository.Execute("INSERT INTO balance_history VALUES(?,?)", (balance, int(time.time()*1000)))

    @staticmethod
    def upsert_symbols(symbols_to_upsert):
        if len(Repository.ExecuteWithResult("SELECT * FROM symbols")) == 0:
            Repository.ExecuteMany("""INSERT INTO symbols(deviation,lin_reg_coef_a,lin_reg_coef_b,is_outside_deviation,last_updated,symbol)
                                      VALUES(?,?,?,0,?,?)""", symbols_to_upsert)
            return
        Repository.ExecuteMany("""UPDATE symbols
                                  SET deviation = ?, lin_reg_coef_a = ?, lin_reg_coef_b = ?, last_updated=?
                                  WHERE symbol == ?""", symbols_to_upsert)
    @staticmethod
    def update_symbol_is_outside_deviation(symbol, is_outside_deviation, time=int(time.time()*1000)):
        Repository.Execute(f"UPDATE symbols SET is_outside_deviation = {is_outside_deviation}, iod_last_updated = {time} WHERE symbol == '{symbol}'")  
    @staticmethod
    def get_active_symbols():
        return Repository.ExecuteWithResult("SELECT * FROM symbols")

    @staticmethod
    def get_last_symbol_information_update():
        querry_result = Repository.ExecuteWithResult("SELECT * FROM symbol_information_updates ORDER BY starting_time DESC")
        return querry_result[0] if len(querry_result) != 0 else None
    @staticmethod
    def add_symbol_information_update(interval, bars, starting_time, time_elapsed):
        Repository.Execute("INSERT INTO symbol_information_updates(_interval,bars,starting_time,time_elapsed) VALUES(?,?,?,?)", (interval, bars, starting_time, time_elapsed))

    @staticmethod
    def add_trade(trade):
        Repository.Execute("""INSERT INTO trades(symbol,openOrderId,closeOrderId,triggeredOrderType,side,enterPrice,exitPrice,qty,realizedPnl,enterCommission,exitCommission,enterTime,exitTime,tradeId) 
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                              (trade['symbol'],
                               trade['openOrderId'],
                               trade['closeOrderId'],
                               trade['triggeredOrderType'],
                               trade['side'],
                               trade['enterPrice'],
                               trade['exitPrice'],
                               trade['qty'],
                               trade['realizedPnl'],
                               trade['enterCommission'],
                               trade['exitCommission'],
                               trade['enterTime'],
                               trade['exitTime'],
                               trade['tradeId']))

    @staticmethod
    def archive_order(order):
        Repository.Execute("INSERT INTO orders_archive(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,leverage) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", order)

    @staticmethod
    def add_order(order):
        Repository.Execute("""INSERT INTO 
                           orders(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,
                                  timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,
                                  priceProtect,origType,updateTime,tradeId,leverage)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                           (order['orderId'], 
                            order['symbol'], 
                            order['status'], 
                            order['clientOrderId'], 
                            order['price'], 
                            order['avgPrice'], 
                            order['origQty'], 
                            order['executedQty'], 
                            order['cumQuote'], 
                            order['timeInForce'], 
                            order['type'], 
                            order['reduceOnly'], 
                            order['closePosition'], 
                            order['side'], 
                            order['positionSide'], 
                            order['stopPrice'], 
                            order['workingType'], 
                            order['priceProtect'], 
                            order['origType'], 
                            order['updateTime'],
                            order['tradeId'],
                            order['leverage']))

    @staticmethod
    def get_all_market_orders():
        return Repository.ExecuteWithResult("SELECT * FROM orders WHERE orders.type == 'MARKET'")

    @staticmethod
    def update_order_current_parameters(current_profit, last_price, symbol):
        Repository.Execute("UPDATE orders SET lastPrice = ?, currentProfit = ? WHERE symbol == ?", (last_price, current_profit, symbol))

    @staticmethod
    def check_if_orders_available(symbol):
        return len(Repository.ExecuteWithResult(f"SELECT * FROM orders WHERE symbol == ?", (symbol.upper(),))) > 0
        
    @staticmethod
    def get_coins_with_open_orders():
        return [o[0] for o in Repository.ExecuteWithResult("SELECT symbol FROM orders")]
        
    @staticmethod
    def get_active_order_by_type(symbol, type):
        return Repository.ExecuteWithResult(f"SELECT * FROM orders WHERE symbol == '{symbol}' AND type LIKE '%{type}%'")[0]

    @staticmethod
    def remove_orders(symbol):
        Repository.Execute(f"DELETE FROM orders WHERE symbol == ?", (symbol,))

    @staticmethod    
    def seed_currencies(currencies):
        Repository.Execute("DELETE FROM currencies")
        Repository.ExecuteMany("INSERT INTO currencies(symbol, price_precision,quantity_precision, minimum_notional, tick_size) VALUES (?,?,?,?,?)", currencies)
    @staticmethod
    def update_currencies(currencies):
        Repository.ExecuteMany("""UPDATE currencies 
                                  SET price_precision=?, quantity_precision=?, minimum_notional=?, tick_size=?
                                  WHERE symbol == ?""", currencies)
    @staticmethod
    def update_leverages(leverages):
        Repository.ExecuteMany("""UPDATE currencies 
                                  SET leverage = ?
                                  WHERE symbol == ?""", leverages)
    @staticmethod
    def update_current_leverage_and_max_notional(symbol, current_leverage, current_max_notional):
        Repository.Execute("""UPDATE currencies 
                                  SET current_leverage = ?, current_max_notional = ?
                                  WHERE symbol == ?""", (current_leverage,current_max_notional,symbol))
    @staticmethod
    def get_current_leverage_and_max_notional(symbol):
        return Repository.ExecuteWithResult("""SELECT current_leverage,current_max_notional
                                               FROM currencies 
                                               WHERE symbol == ?""", [symbol])[0]
    @staticmethod
    def get_all_currencies():
        return Repository.ExecuteWithResult("SELECT * FROM currencies")
    @staticmethod
    def get_currency(symbol):
        return Repository.ExecuteWithResult(f"SELECT * FROM currencies WHERE symbol == ?", (symbol,))[0]

    @staticmethod
    def add_weight(weight, method, time):
        Repository.Execute("INSERT INTO used_weights(weight,time,method) VALUES(?,?,?)", (weight, (int(time)), method))    
    @staticmethod
    def get_weight_for_last_minute(current_time):
        minute_ago = current_time - 60000
        return sum([w[0] for w in Repository.ExecuteWithResult("SELECT weight FROM used_weights WHERE time >= ?", [minute_ago])])
    @staticmethod
    def update_weight_time(old_time, new_time):
        Repository.Execute("UPDATE used_weights SET time = ? WHERE time == ?", (new_time, old_time))
    @staticmethod
    def get_all_used_weights():
        return Repository.ExecuteWithResult("SELECT * FROM used_weights")
    @staticmethod
    def delete_old_weights():
        #delete all weights that older than 1 hour
        if Repository.CURRENT_DATABASE == Repository.SQLITE:
            Repository.Execute("DELETE FROM used_weights Where used_weights.time <= (strftime('%s', 'now')*1000 - 3600*1000)")
        elif Repository.CURRENT_DATABASE == Repository.MYSQL:
            Repository.Execute("DELETE FROM used_weights Where used_weights.time <= (UNIX_TIMESTAMP(now())*1000 - 3600*1000)")
    
    @staticmethod
    def log_message(message):
        Repository.Execute("INSERT INTO logs(time,message) VALUES(?,?)",(int(time.time()*1000), message))    

    @staticmethod
    def validate_mysql_querry(querry:str):
        return querry.replace('==', '=').replace("OR IGNORE", "IGNORE")  

    @staticmethod
    def get_symbol_info(symbol:str):
        return Repository.ExecuteWithResult(f"SELECT deviation, lin_reg_coef_a, lin_reg_coef_b FROM symbols WHERE symbol == ?", (symbol))[0]
    @staticmethod
    def get_balance_history():
        return Repository.ExecuteWithResult("SELECT * FROM balance_history")
    @staticmethod
    def get_available_balance_history():
        return Repository.ExecuteWithResult("SELECT * FROM available_balance_history")

def get_currencies_querry():
    querry = """INSERT INTO currencies(symbol,price_precision,quantity_precision,minimum_notional,tick_size,leverage) VALUES"""
    for c in Repository.get_all_currencies():
        querry += f"""("{c[0]}",{c[1]},{c[2]},{c[3]},{c[4]},{c[5]}),"""
    querry = querry.removesuffix(',')
    querry += ';'
    return querry


if __name__ == '__main__':
    
    pass
