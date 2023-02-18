import sqlite3
import time
from config_manager import ConfigManager
from numpy import double
import mysql.connector

def interval_to_time(interval):
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
def time_to_interval(time):
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
    hedgeId = 21
    leverage = 22
    lastPrice =	23
    currentProfit = 24

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
    def get_virtual_balance_history():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM virtual_balance_history")
    @staticmethod
    def get_balance_history():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM balance_history")
    @staticmethod
    def get_all_currencies():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM currencies")

    @staticmethod
    def add_backtest_cointegrations(values):
        BacktestRepository.ExecuteMany("INSERT INTO cointegrations(time,adf,eg,deviation,lin_reg_coef_a,lin_reg_coef_b,hurst_exponent,_interval,pairId) VALUES(?,?,?,?,?,?,?,?,?)", values)

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
    def get_backtest_pairId(coin1, coin2):
        return BacktestRepository.ExecuteWithResult(f"SELECT id FROM pairs WHERE pair == '{coin1}/{coin2}'")[0][0]
    
    @staticmethod
    def add_klines(symbol, klines, interval):
        int_interval = interval_to_time(interval)
        result = [(symbol, k[0], k[1], k[2], k[3], k[4], k[6], int_interval) for k in klines]
        BacktestRepository.ExecuteMany(f"INSERT INTO klines_{interval}(symbol,open_time,open,high,low,close,close_time,_interval) VALUES(?,?,?,?,?,?,?,?)", result)

    pass
    @staticmethod
    def get_virtual_kotleta():
        return double(BacktestRepository.get_variable('virtual_kotleta'))
    @staticmethod    
    def set_virtual_kotleta(value, time = int(time.time()*1000)):
        BacktestRepository.update_variable('virtual_kotleta', value)
        BacktestRepository.add_virtual_balance_snapshot(value, time)
    @staticmethod
    def add_to_virtual_kotleta(value, time = int(time.time()*1000)):
        BacktestRepository.Execute(f"UPDATE variables SET value=value+{value} WHERE name == 'virtual_kotleta'")
        BacktestRepository.add_virtual_balance_snapshot(BacktestRepository.get_virtual_kotleta(), time)

    @staticmethod
    def add_virtual_balance_snapshot(balance, time):
        BacktestRepository.Execute("INSERT INTO virtual_balance_history(balance, change_time) VALUES(?,?)", (balance, time))

    @staticmethod
    def get_kotleta():
        return double(BacktestRepository.get_variable('kotleta'))
    @staticmethod    
    def set_kotleta(value, time):
        BacktestRepository.update_variable('kotleta', value)
        BacktestRepository.add_balance_snapshot(value, time)      
    @staticmethod    
    def add_to_kotleta(value, time):
        BacktestRepository.Execute("UPDATE variables SET value=value+? WHERE name == 'kotleta'", (value,))
        BacktestRepository.add_balance_snapshot(BacktestRepository.get_kotleta(), time)  
    @staticmethod
    def get_backtest_hedge_limit():
        return int(BacktestRepository.get_variable('backtest_hedge_limit'))
    @staticmethod    
    def set_backtest_hedge_limit(value):
        BacktestRepository.update_variable('backtest_hedge_limit', value)
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
    def get_hedges_launched():
        return int(BacktestRepository.get_variable('hedges_launched'))
    @staticmethod    
    def set_hedges_launched(value):
        BacktestRepository.update_variable('hedges_launched', value) 
    @staticmethod  
    def get_burst_starting_balance():
        return BacktestRepository.get_variable('burst_starting_balance')
    @staticmethod    
    def set_burst_starting_balance(value):
        if value is None:
            value = 'NULL'
        BacktestRepository.update_variable('burst_starting_balance', value)   
    @staticmethod
    def get_variable(name):
        return BacktestRepository.ExecuteWithResult(f"SELECT value FROM variables WHERE name == '{name}'")[0][0]
    @staticmethod
    def update_variable(name, value):
        BacktestRepository.Execute(f"UPDATE variables SET value = {value} WHERE name == '{name}'")
    @staticmethod
    def add_balance_snapshot(balance, time = int(time.time()*1000)):
        BacktestRepository.Execute("INSERT INTO balance_history(balance,change_time) VALUES(?,?)", (balance, time))

    @staticmethod
    def seed_database():
        BacktestRepository.Execute("DELETE FROM logs")  
        BacktestRepository.Execute("DELETE FROM orders")    
        BacktestRepository.Execute("DELETE FROM backtest_results")
        BacktestRepository.Execute("DELETE FROM orders_archive")  
        BacktestRepository.Execute("DELETE FROM balance_history")   
        BacktestRepository.Execute("DELETE FROM virtual_balance_history")  
        #BacktestRepository.Execute("INSERT OR IGNORE INTO variables VALUES('is_program_shutdown_started', 0)") 
        BacktestRepository.set_kotleta(9.25382, 0)
        BacktestRepository.set_virtual_kotleta(9.25382, 0)
        BacktestRepository.set_backtest_hedge_limit(-1)
        BacktestRepository.set_is_order_creation_allowed(True)
        BacktestRepository.set_is_program_shutdown_started(False)
        BacktestRepository.set_hedges_launched(0)

    @staticmethod
    def delete_uncointegrated_pairs():
        coins_with_active_orders = BacktestRepository.get_coins_with_open_orders_by_hedges()
        command = "DELETE FROM active_pairs WHERE failed_cointegrations > 10"
        for coin1, coin2 in coins_with_active_orders:
            command += f" AND pair != '{coin1}/{coin2}'"
        BacktestRepository.Execute(command) 
    @staticmethod
    def delete_pair(pair:str):
        BacktestRepository.Execute("DELETE FROM active_pairs WHERE pair == ? AND adf > ?", [pair, ConfigManager.config['adf_value_threshold']]) 
    @staticmethod
    #dict[str:tuple[float, float]]
    def get_cointegrations(time:int):
        querry_result = BacktestRepository.ExecuteWithResult(f"""SELECT pair, adf, eg, deviation, lin_reg_coef_a, lin_reg_coef_b, hurst_exponent, time 
                                                                FROM cointegrations 
                                                                INNER JOIN pairs ON pairs.id = cointegrations.pairId 
                                                                WHERE cointegrations.time == {time}
                                                                ORDER BY pairId, time""")
        return querry_result


    @staticmethod
    def get_active_pairs_symbols():
        querry_result = BacktestRepository.ExecuteWithResult("SELECT pair FROM active_pairs")
        return list([r[0] for r in querry_result])

    @staticmethod
    def add_active_pairs(pairs:list):
        pairs_to_import = []
        #pair, adf, eg, deviation, lin_reg_coef_a, lin_reg_coef_b, time
        for pair in pairs:
            pairs_to_import.append((pair[0],   #pair
                                    pair[1],   #adf
                                    #pair[2],   #EG
                                    0,         #failed cointegrations
                                    0,         #is outside deviation
                                    pair[3],   #deviation
                                    pair[4],   #lin_reg_coef_a
                                    pair[5],   #lin_reg_coef_b
                                    #pair[6],   #hurst_exponent
                                    pair[7]))  #time
        BacktestRepository.ExecuteMany("INSERT OR IGNORE INTO active_pairs(pair,adf,failed_cointegrations,is_outside_deviations,deviation,lin_reg_coef_a,lin_reg_coef_b,last_updated) VALUES(?,?,?,?,?,?,?,?)", pairs_to_import)


    @staticmethod
    def update_active_pairs(pairs:list):
        pairs_to_import = []
        #pair, adf, eg, deviation, lin_reg_coef_a, lin_reg_coef_b, time
        for pair in pairs:
            pairs_to_import.append((
                                    pair[1],  #adf | pair[2] - EG
                                    (0,1)[pair[1] > ConfigManager.config['adf_value_threshold'] or pair[2] > ConfigManager.config['adf_value_threshold']],  #failed_cointegrations
                                    pair[3],  #deviation
                                    pair[4],  #a
                                    pair[5],  #b
                                    #pair[6],   #hurst_exponent
                                    pair[7],  #time
                                    pair[0])) #pair
        BacktestRepository.ExecuteMany("""
                        UPDATE active_pairs 
                        SET adf=?, failed_cointegrations=failed_cointegrations+?, deviation=?, lin_reg_coef_a=?, lin_reg_coef_b=?, last_updated=?
                        WHERE pair == ?""", pairs_to_import)

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
    def add_backtest_result(symbol, triggered_order_type, profit, exit_time, enter_time, tradeId, hedgeId):
        BacktestRepository.Execute("INSERT INTO backtest_results(symbol,triggered_order_type,profit,exit_time,enter_time,tradeId,hedgeId) VALUES(?,?,?,?,?,?,?)", (symbol, triggered_order_type, profit, exit_time, enter_time, tradeId, hedgeId) )

    @staticmethod
    def remove_orders(symbol):
        BacktestRepository.Execute(f"DELETE FROM orders WHERE symbol == '{symbol}'")

    @staticmethod
    def archive_order(order):
        BacktestRepository.Execute("INSERT INTO orders_archive(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,hedgeId,leverage) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", order)

    @staticmethod
    def get_active_pairs():
        return BacktestRepository.ExecuteWithResult("SELECT * FROM active_pairs ORDER BY adf")


    @staticmethod
    def check_if_orders_available(symbol):
        querry_result = BacktestRepository.ExecuteWithResult(f"SELECT * FROM orders WHERE symbol == '{symbol.upper()}'")
        return len(querry_result) > 0    
    @staticmethod
    def update_pair_is_outside_deviations(pair, is_outside_deviations):
        BacktestRepository.Execute(f"UPDATE active_pairs SET is_outside_deviations = {is_outside_deviations} WHERE pair LIKE '%{pair}%'")   
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
        BacktestRepository.Execute("""INSERT INTO orders(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,hedgeId,leverage,lastPrice,currentProfit)
                                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (order['orderId'], 
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
                                                         order['hedgeId'],
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
    def get_pair_by_coins(coin1:str, coin2:str):
        return BacktestRepository.ExecuteWithResult(f"SELECT pair, deviation, lin_reg_coef_a, lin_reg_coef_b FROM active_pairs WHERE pair LIKE '%{coin1}%' AND pair LIKE '%{coin2}%'")[0]
    
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
    CURRENT_DATABASE = SQLITE
    db = sqlite3.connect(ConfigManager.config['connection_string']) if CURRENT_DATABASE == SQLITE else mysql.connector.connect(
        host="localhost",
        #host='47.243.57.109',
        user=ConfigManager.config['mysql_login'],
        password=ConfigManager.config['mysql_password'],
        database=ConfigManager.config['mysql_database'],
        autocommit=True
)


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
    def seed_database(ONLINE_TEST_MODE:bool):
        Repository.Execute("DELETE FROM logs") 
        Repository.Execute("DELETE FROM cointegrations")    
        Repository.Execute("DELETE FROM pairs") 
        Repository.Execute("DELETE FROM trades") 
        Repository.Execute("DELETE FROM balance_history") 
        Repository.Execute("DELETE FROM virtual_balance_history")     
        Repository.Execute("DROP TABLE orders") 
        if ONLINE_TEST_MODE:
            if Repository.CURRENT_DATABASE == Repository.SQLITE:
                Repository.Execute("""CREATE TABLE "orders"("orderId"	INTEGER,"symbol"	TEXT,"status"	TEXT,"clientOrderId"	TEXT,"price"	REAL,"avgPrice"	REAL,"origQty"	REAL,"executedQty"	REAL,"cumQuote"	REAL,"timeInForce"	TEXT,"type"	TEXT,"reduceOnly"	INTEGER,"closePosition"	INTEGER,"side"	TEXT,"positionSide"	TEXT,"stopPrice"	REAL,"workingType"	TEXT,"priceProtect"	INTEGER,"origType"	TEXT,"updateTime"	INTEGER,"tradeId"	INTEGER,"hedgeId"	INTEGER,"leverage"	INTEGER,"lastPrice"	REAL,"currentProfit"	REAL);""")     
            elif Repository.CURRENT_DATABASE == Repository.MYSQL:
                Repository.Execute("""CREATE TABLE orders (orderId BIGINT,symbol VARCHAR(20), status VARCHAR(32), clientOrderId VARCHAR(64),price REAL,avgPrice REAL,origQty REAL,executedQty REAL,cumQuote REAL,timeInForce VARCHAR(10),type VARCHAR(20),reduceOnly INTEGER,closePosition INTEGER,side VARCHAR(10),positionSide VARCHAR(10),stopPrice REAL,workingType VARCHAR(32),priceProtect INTEGER,origType VARCHAR(20),updateTime BIGINT,tradeId BIGINT,hedgeId BIGINT,leverage INTEGER,lastPrice REAL,currentProfit REAL)ENGINE=INNODB;""")
            Repository.set_kotleta(1000.0)
            Repository.set_virtual_kotleta(1000.0)
        else:
            if Repository.CURRENT_DATABASE == Repository.SQLITE:
                Repository.Execute("""CREATE TABLE "orders"("orderId"	INTEGER,"symbol"	TEXT,"status"	TEXT,"clientOrderId"	TEXT,"price"	REAL,"avgPrice"	REAL,"origQty"	REAL,"executedQty"	REAL,"cumQuote"	REAL,"timeInForce"	TEXT,"type"	TEXT,"reduceOnly"	INTEGER,"closePosition"	INTEGER,"side"	TEXT,"positionSide"	TEXT,"stopPrice"	REAL,"workingType"	TEXT,"priceProtect"	INTEGER,"origType"	TEXT,"updateTime"	INTEGER,"tradeId"	INTEGER,"hedgeId"	INTEGER,"leverage"	INTEGER);""")      
            elif Repository.CURRENT_DATABASE == Repository.MYSQL:
                Repository.Execute("""CREATE TABLE orders (orderId BIGINT,symbol VARCHAR(20), status VARCHAR(32), clientOrderId VARCHAR(64),price REAL,avgPrice REAL,origQty REAL,executedQty REAL,cumQuote REAL,timeInForce VARCHAR(10),type VARCHAR(20),reduceOnly INTEGER,closePosition INTEGER,side VARCHAR(10),positionSide VARCHAR(10),stopPrice REAL,workingType VARCHAR(32),priceProtect INTEGER,origType VARCHAR(20),updateTime BIGINT,tradeId BIGINT,hedgeId BIGINT,leverage INTEGER)ENGINE=INNODB;""")
        Repository.Execute("DELETE FROM test_results")
        Repository.Execute("DELETE FROM orders_archive")  
        
        Repository.set_backtest_hedge_limit(-1)
        Repository.set_is_order_creation_allowed(True)
        Repository.set_is_program_shutdown_started(False)
        Repository.set_hedges_launched(0)

        


    @staticmethod
    def get_kotleta():
        return double(Repository.get_variable('kotleta'))
    @staticmethod    
    def set_kotleta(value):
        Repository.update_variable('kotleta', value)
        Repository.add_balance_snapshot(value)    
    @staticmethod    
    def set_virtual_kotleta(value):
        Repository.update_variable('virtual_kotleta', value)
        Repository.add_virtual_balance_snapshot(value)        
    @staticmethod
    def get_virtual_kotleta():
        return double(Repository.get_variable('virtual_kotleta'))
    @staticmethod
    def add_to_virtual_kotleta(value):
        Repository.set_virtual_kotleta(Repository.get_virtual_kotleta()+value)

    @staticmethod
    def get_backtest_hedge_limit():
        return int(Repository.get_variable('backtest_hedge_limit'))
    @staticmethod    
    def set_backtest_hedge_limit(value):
        Repository.update_variable('backtest_hedge_limit', value)
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
    def get_hedges_launched():
        return int(Repository.get_variable('hedges_launched'))
    @staticmethod    
    def set_hedges_launched(value):
        Repository.update_variable('hedges_launched', value)  
    @staticmethod
    def get_variable(name):
        return Repository.ExecuteWithResult(f"SELECT value FROM variables WHERE name == '{name}'")[0][0]  
    @staticmethod
    def update_variable(name, value):
        Repository.Execute(f"UPDATE variables SET value = {value} WHERE name == '{name}'")
    @staticmethod
    def add_balance_snapshot(balance):
        Repository.Execute("INSERT INTO balance_history VALUES(?,?)", (balance, int(time.time()*1000)))
    @staticmethod
    def add_virtual_balance_snapshot(balance):
        Repository.Execute("INSERT INTO virtual_balance_history VALUES(?,?)", (balance, int(time.time()*1000)))

    @staticmethod
    def add_pairs(pairs_to_import):
        Repository.ExecuteMany("""INSERT OR IGNORE 
                                  INTO pairs(pair,adf,failed_cointegrations,is_outside_deviations,deviation,lin_reg_coef_a,lin_reg_coef_b,last_updated) 
                                  VALUES(?,?,?,?,?,?,?,?)""", pairs_to_import)

    @staticmethod
    def delete_uncointegrated_pairs():
        coins_with_active_orders = [c[0] for c in Repository.ExecuteWithResult("SELECT DISTINCT symbol FROM orders")]
        command = "DELETE FROM pairs WHERE failed_cointegrations > 10"
        for coin in coins_with_active_orders:
            command += f" AND pair NOT LIKE '%{coin}%'"
        Repository.Execute(command) 

    @staticmethod
    def update_pair_is_outside_deviations(pair, is_outside_deviations):
        Repository.Execute("UPDATE pairs SET is_outside_deviations=?, last_updated=? WHERE pair == ?", (is_outside_deviations, int(time.time()*1000), pair))    

    @staticmethod
    def update_pairs(pairs_to_update):
        Repository.ExecuteMany("""UPDATE pairs
                                  SET adf=?, failed_cointegrations=failed_cointegrations + ?, 
                                  deviation = ?, lin_reg_coef_a = ?, lin_reg_coef_b = ?, last_updated=?
                                  WHERE pair == ?""", pairs_to_update)
    @staticmethod
    def set_pairs_default_is_outside_deviation():
        Repository.Execute("UPDATE pairs SET is_outside_deviations = 0")
    @staticmethod
    def get_pairs():
        return Repository.ExecuteWithResult("SELECT * FROM pairs ORDER BY adf")
    @staticmethod
    def get_pair_combinations():
        return [r[0] for r in Repository.ExecuteWithResult("SELECT pair FROM pair_combinations")]
    @staticmethod
    def get_pair(pair):
        return Repository.ExecuteWithResult(f"SELECT pair,deviation,lin_reg_coef_a, lin_reg_coef_b FROM pairs WHERE pair == '{pair}'")[0]
    @staticmethod
    def delete_pair(pair):
        Repository.Execute("DELETE FROM pairs WHERE pair == ? AND adf > ?", [pair, ConfigManager.config['adf_value_threshold']])



        Repository.db.commit()

    #Returns None if no last cointegration
    @staticmethod
    def get_last_cointegration():
        querry_result = Repository.ExecuteWithResult("SELECT * FROM cointegrations ORDER BY starting_time DESC")
        return querry_result[0] if len(querry_result) != 0 else None
        

    @staticmethod
    def add_cointegration(interval, bars, starting_time, time_elapsed):
        Repository.Execute("INSERT INTO cointegrations(_interval,bars,starting_time,time_elapsed) VALUES(?,?,?,?)", (interval, bars, starting_time, time_elapsed))

    @staticmethod
    def add_trade(trade):
        Repository.Execute("""INSERT INTO trades(symbol,openOrderId,closeOrderId,triggeredOrderType,side,enterPrice,exitPrice,qty,realizedPnl,enterCommission,exitCommission,enterTime,exitTime,hedgeId) 
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
                               trade['hedgeId']))

    @staticmethod
    def get_all_trades():
        c=Repository.db.cursor()
        c.execute("SELECT * FROM trades")
        return c.fetchall()
        pass

    @staticmethod
    def archive_order(order):
        Repository.Execute("INSERT INTO orders_archive(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,hedgeId,leverage) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", order)

    @staticmethod
    def add_test_order(order):
        Repository.Execute("INSERT INTO orders(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,hedgeId,leverage,lastPrice,currentProfit) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (order['orderId'], 
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
                                                         order['hedgeId'],
                                                         order['leverage'],
                                                         order['lastPrice'],
                                                         order['currentProfit']))
    @staticmethod
    def add_order(order):
        Repository.Execute("INSERT INTO orders(orderId,symbol,status,clientOrderId,price,avgPrice,origQty,executedQty,cumQuote,timeInForce,type,reduceOnly,closePosition,side,positionSide,stopPrice,workingType,priceProtect,origType,updateTime,tradeId,hedgeId,leverage) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (order['orderId'], 
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
                                                         order['hedgeId'],
                                                         order['leverage']))

    @staticmethod
    def update_order_current_parameters(current_profit, last_price, symbol):
        Repository.Execute("UPDATE orders SET lastPrice = ?, currentProfit = ? WHERE symbol == ?", (last_price, current_profit, symbol))


    @staticmethod
    def check_if_orders_available(symbol):
        return len(Repository.ExecuteWithResult(f"SELECT * FROM orders WHERE symbol == '{symbol.upper()}'")) > 0
        
    @staticmethod
    def get_coins_with_open_orders():
        return [o[0] for o in Repository.ExecuteWithResult("SELECT symbol FROM orders")]
    @staticmethod
    def get_coins_with_open_orders_by_hedges():
        hedges = [h[0] for h in Repository.ExecuteWithResult("SELECT DISTINCT hedgeId FROM orders")]
        if len(hedges) == 1 and hedges[0] is None:
            return []
        if Repository.CURRENT_DATABASE == Repository.SQLITE:
            return [Repository.ExecuteWithResult("SELECT * FROM (SELECT substr(pair, 1, pos-1) AS coin1, substr(pair, pos+1) AS coin2 FROM (SELECT *, instr(pair, '/') AS pos FROM pairs)) WHERE coin1 IN (SELECT symbol FROM orders WHERE hedgeId == ?) AND coin2 IN (SELECT symbol FROM orders WHERE hedgeId == ?)", [hedge, hedge])[0] for hedge in hedges]
        if Repository.CURRENT_DATABASE == Repository.MYSQL:
            return [Repository.ExecuteWithResult("SELECT * FROM (SELECT SPLIT_STR(pair, '/', 1) as coin1, SPLIT_STR(pair, '/', 2) as coin2 FROM pair_combinations) as c WHERE coin1 IN (SELECT symbol FROM orders WHERE hedgeId = ?) AND coin2 IN (SELECT symbol FROM orders WHERE hedgeId = ?);", [hedge, hedge])[0] for hedge in hedges]
        #return {hedge:Repository.ExecuteWithResult("SELECT symbol FROM orders WHERE hedgeId == ?", [hedge]) for hedge in hedges}

    @staticmethod
    def get_active_order_by_type(symbol, type):
        return Repository.ExecuteWithResult(f"SELECT * FROM orders WHERE symbol == '{symbol}' AND type LIKE '%{type}%'")[0]


    @staticmethod
    def get_all_orders():
        return Repository.ExecuteWithResult("SELECT * FROM orders")
    @staticmethod
    def remove_orders(symbol):
        Repository.Execute(f"DELETE FROM orders WHERE symbol == '{symbol}'")

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
        return Repository.ExecuteWithResult(f"SELECT * FROM currencies WHERE symbol == '{symbol}'")[0]

    """CREATE TABLE used_weights(
        weight INTEGER,
        time INTEGER, 
        method TEXT)"""
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
    def add_test_result(symbol, triggered_order_type, profit, enter_price, exit_price, enter_time, exit_time, tradeId, hedgeId, leverage):
        Repository.Execute("INSERT INTO test_results(symbol, triggered_order_type, profit, enter_price, exit_price, enter_time, exit_time, tradeId, hedgeId, leverage) VALUES(?,?,?,?,?,?,?,?,?,?)", 
                                                    (symbol, triggered_order_type, profit, enter_price, exit_price, enter_time, exit_time, tradeId, hedgeId, leverage) )

    @staticmethod
    def validate_mysql_querry(querry:str):
        return querry.replace('==', '=').replace("OR IGNORE", "IGNORE")  

    @staticmethod
    def remove_coin(coin):
        Repository.Execute(f"DELETE FROM pair_combinations WHERE pair LIKE '%{coin}%';")
        Repository.Execute(f"DELETE FROM currencies WHERE symbol == '{coin}';")

    @staticmethod
    def get_pair_by_coins(coin1, coin2):
        return Repository.ExecuteWithResult(f"SELECT pair, lin_reg_coef_a, lin_reg_coef_b FROM pairs WHERE pair LIKE '%{coin1}%' AND pair LIKE '%{coin2}%' ")[0]
    @staticmethod
    def get_virtual_balance_history():
        return Repository.ExecuteWithResult("SELECT * FROM virtual_balance_history")
    @staticmethod
    def get_balance_history():
        return Repository.ExecuteWithResult("SELECT * FROM balance_history")

def get_currencies_querry():
    querry = """INSERT INTO currencies(symbol,price_precision,quantity_precision,minimum_notional,tick_size,leverage) VALUES"""
    for c in Repository.get_all_currencies():
        querry += f"""("{c[0]}",{c[1]},{c[2]},{c[3]},{c[4]},{c[5]}),"""
    querry = querry.removesuffix(',')
    querry += ';'
    return querry

def get_pairs_querry():
    querry = """INSERT INTO pair_combinations(pair) VALUES"""
    for pair in Repository.ExecuteWithResult("SELECT pair FROM pair_combinations"):
        querry += f"""("{pair[0]}"),"""
    querry = querry.removesuffix(',')
    querry += ';'
    return querry

if __name__ == '__main__':
    BacktestRepository.Execute("DELETE FROM cointegrations;")
    BacktestRepository.Execute("VACUUM;")
    pass
