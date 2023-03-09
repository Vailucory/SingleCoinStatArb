USE SingleCoinStatArb;
DROP TABLE IF EXISTS available_balance_history;
DROP TABLE IF EXISTS balance_history;
DROP TABLE IF EXISTS currencies;
DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS orders_archive;
DROP TABLE IF EXISTS symbols;
DROP TABLE IF EXISTS symbol_information_updates;
DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS used_weights;
DROP TABLE IF EXISTS variables;

CREATE TABLE available_balance_history(balance REAL,change_time BIGINT)ENGINE=INNODB;
CREATE TABLE balance_history(
	balance REAL,
	change_time BIGINT) ENGINE=INNODB;
CREATE TABLE currencies(
    symbol VARCHAR(20) UNIQUE,
    price_precision INTEGER,
    quantity_precision INTEGER, 
    minimum_notional REAL,
    tick_size REAL, 
    leverage INTEGER,
	current_leverage INTEGER,
	current_max_notional REAL) ENGINE=INNODB;
CREATE TABLE logs(time BIGINT, message VARCHAR(4096)) ENGINE=INNODB;
CREATE TABLE orders (
    orderId    BIGINT,
    symbol    VARCHAR(20),
    status    VARCHAR(32),
    clientOrderId    VARCHAR(64),
    price    REAL,
    avgPrice    REAL,
    origQty    REAL,
    executedQty    REAL,
    cumQuote    REAL,
    timeInForce    VARCHAR(10),
    type    VARCHAR(20),
    reduceOnly    INTEGER,
    closePosition    INTEGER,
    side    VARCHAR(10),
    positionSide    VARCHAR(10),
    stopPrice    REAL,
    workingType    VARCHAR(32),
    priceProtect    INTEGER,
    origType    VARCHAR(20),
    updateTime    BIGINT,
    tradeId    BIGINT,
    leverage    INTEGER,
    lastPrice    REAL,
    currentProfit    REAL)ENGINE=INNODB;   
CREATE TABLE orders_archive (
    orderId    BIGINT,
    symbol    VARCHAR(20),
    status    VARCHAR(32),
    clientOrderId    VARCHAR(64),
    price    REAL,
    avgPrice    REAL,
    origQty    REAL,
    executedQty    REAL,
    cumQuote    REAL,
    timeInForce    VARCHAR(10),
    type    VARCHAR(20),
    reduceOnly    INTEGER,
    closePosition    INTEGER,
    side    VARCHAR(10),
    positionSide    VARCHAR(10),
    stopPrice    REAL,
    workingType    VARCHAR(32),
    priceProtect    INTEGER,
    origType    VARCHAR(20),
    updateTime    BIGINT,
    tradeId    BIGINT,
    leverage    INTEGER)ENGINE=INNODB;
CREATE TABLE symbols(
        symbol VARCHAR(40) UNIQUE,
        deviation REAL,
        lin_reg_coef_a REAL, 
        lin_reg_coef_b REAL, 
        is_outside_deviation INTEGER,
        last_price REAL,
        iod_last_updated BIGINT,
        last_updated BIGINT 
        ) ENGINE=INNODB;
CREATE TABLE symbol_information_updates(
        _interval VARCHAR(8),
        bars INTEGER,
        starting_time BIGINT,
        time_elapsed INTEGER) ENGINE=INNODB;
CREATE TABLE trades (
	symbol	VARCHAR(20),
	openOrderId	BIGINT,
	closeOrderId	BIGINT,
	triggeredOrderType	VARCHAR(32),
	side	VARCHAR(8),
	enterPrice	REAL,
	exitPrice	REAL,
	qty	REAL,
	realizedPnl	REAL,
	enterCommission	REAL,
	exitCommission	REAL,
	enterTime	BIGINT,
	exitTime	BIGINT,
	tradeId	BIGINT) ENGINE=INNODB;

CREATE TABLE used_weights(
    weight INTEGER,
    time BIGINT, 
    method VARCHAR(40)) ENGINE=INNODB;
CREATE TABLE variables (
	name	VARCHAR(32) UNIQUE,
	value	REAL)ENGINE=INNODB;

INSERT INTO currencies(symbol,price_precision,quantity_precision,minimum_notional,tick_size,leverage) VALUES("BTCUSDT",2,3,5.0,0.1,20),("ETHUSDT",2,3,5.0,0.01,20),("BCHUSDT",2,3,5.0,0.01,20),("XRPUSDT",4,1,5.0,0.0001,20),("EOSUSDT",3,1,5.0,0.001,20),("LTCUSDT",2,3,5.0,0.01,20),("TRXUSDT",5,0,5.0,1e-05,20),("ETCUSDT",3,2,5.0,0.001,20),("LINKUSDT",3,2,5.0,0.001,20),("XLMUSDT",5,0,5.0,1e-05,20),("ADAUSDT",5,0,5.0,0.0001,20),("XMRUSDT",2,3,5.0,0.01,20),("DASHUSDT",2,3,5.0,0.01,20),("ZECUSDT",2,3,5.0,0.01,20),("XTZUSDT",3,1,5.0,0.001,20),("BNBUSDT",3,2,5.0,0.01,20),("ATOMUSDT",3,2,5.0,0.001,20),("ONTUSDT",4,1,5.0,0.0001,20),("IOTAUSDT",4,1,5.0,0.0001,20),("BATUSDT",4,1,5.0,0.0001,20),("VETUSDT",6,0,5.0,1e-05,20),("NEOUSDT",3,2,5.0,0.001,20),("QTUMUSDT",3,1,5.0,0.001,20),("IOSTUSDT",6,0,5.0,1e-06,20),("THETAUSDT",4,1,5.0,0.0001,20),("ALGOUSDT",4,1,5.0,0.0001,20),("ZILUSDT",5,0,5.0,1e-05,20),("KNCUSDT",5,0,5.0,0.0001,20),("ZRXUSDT",4,1,5.0,0.0001,20),("COMPUSDT",2,3,5.0,0.01,20),("OMGUSDT",4,1,5.0,0.001,20),("DOGEUSDT",6,0,5.0,1e-05,20),("SXPUSDT",4,1,5.0,0.0001,20),("KAVAUSDT",4,1,5.0,0.0001,20),("BANDUSDT",4,1,5.0,0.0001,20),("RLCUSDT",4,1,5.0,0.0001,20),("WAVESUSDT",4,1,5.0,0.0001,10),("MKRUSDT",2,3,5.0,0.1,20),("SNXUSDT",3,1,5.0,0.001,20),("DOTUSDT",3,1,5.0,0.001,20),("DEFIUSDT",1,3,5.0,0.1,20),("YFIUSDT",1,3,5.0,1.0,20),("BALUSDT",3,1,5.0,0.001,20),("CRVUSDT",3,1,5.0,0.001,20),("TRBUSDT",3,1,5.0,0.01,20),("RUNEUSDT",4,0,5.0,0.001,20),("SUSHIUSDT",4,0,5.0,0.001,20),("EGLDUSDT",3,1,5.0,0.01,20),("SOLUSDT",4,0,5.0,0.001,12),("ICXUSDT",4,0,5.0,0.0001,20),("STORJUSDT",4,0,5.0,0.0001,20),("BLZUSDT",5,0,5.0,1e-05,20),("UNIUSDT",4,0,5.0,0.001,20),("AVAXUSDT",4,0,5.0,0.001,20),("FTMUSDT",6,0,5.0,0.0001,20),("HNTUSDT",4,0,5.0,0.001,11),("ENJUSDT",5,0,5.0,0.0001,20),("FLMUSDT",4,0,5.0,0.0001,20),("TOMOUSDT",4,0,5.0,0.0001,20),("RENUSDT",5,0,5.0,1e-05,10),("KSMUSDT",3,1,5.0,0.01,20),("NEARUSDT",4,0,5.0,0.001,20),("AAVEUSDT",3,1,5.0,0.01,20),("FILUSDT",3,1,5.0,0.001,20),("RSRUSDT",6,0,5.0,1e-06,20),("LRCUSDT",5,0,5.0,0.0001,20),("MATICUSDT",5,0,5.0,0.0001,20),("OCEANUSDT",5,0,5.0,1e-05,20),("BELUSDT",5,0,5.0,0.0001,20),("CTKUSDT",5,0,5.0,0.0001,20),("AXSUSDT",5,0,5.0,0.001,20),("ALPHAUSDT",5,0,5.0,1e-05,20),("ZENUSDT",3,1,5.0,0.001,20),("SKLUSDT",5,0,5.0,1e-05,20),("GRTUSDT",5,0,5.0,1e-05,20),("1INCHUSDT",4,0,5.0,0.0001,20),("CHZUSDT",5,0,5.0,1e-05,20),("SANDUSDT",5,0,5.0,0.0001,20),("ANKRUSDT",6,0,5.0,1e-05,20),("LITUSDT",3,1,5.0,0.001,20),("UNFIUSDT",3,1,5.0,0.001,20),("REEFUSDT",6,0,5.0,1e-06,20),("RVNUSDT",5,0,5.0,1e-05,20),("SFPUSDT",4,0,5.0,0.0001,8),("XEMUSDT",4,0,5.0,0.0001,20),("COTIUSDT",5,0,5.0,1e-05,20),("CHRUSDT",4,0,5.0,0.0001,20),("MANAUSDT",4,0,5.0,0.0001,20),("ALICEUSDT",3,1,5.0,0.001,20),("HBARUSDT",5,0,5.0,1e-05,20),("ONEUSDT",5,0,5.0,1e-05,20),("LINAUSDT",5,0,5.0,1e-05,20),("STMXUSDT",5,0,5.0,1e-05,20),("DENTUSDT",6,0,5.0,1e-06,10),("CELRUSDT",5,0,5.0,1e-05,20),("HOTUSDT",6,0,5.0,1e-06,20),("MTLUSDT",4,0,5.0,0.0001,20),("OGNUSDT",4,0,5.0,0.0001,20),("NKNUSDT",5,0,5.0,1e-05,20),("DGBUSDT",5,0,5.0,1e-05,20),("1000SHIBUSDT",6,0,5.0,1e-06,20),("BAKEUSDT",4,0,5.0,0.0001,20),("GTCUSDT",3,1,5.0,0.001,20),("BTCDOMUSDT",1,3,5.0,0.1,20),("IOTXUSDT",5,0,5.0,1e-05,20),("AUDIOUSDT",4,0,5.0,0.0001,20),("C98USDT",4,0,5.0,0.0001,20),("MASKUSDT",4,0,5.0,0.001,20),("ATAUSDT",4,0,5.0,0.0001,20),("DYDXUSDT",3,1,5.0,0.001,20),("1000XECUSDT",5,0,5.0,1e-05,20),("GALAUSDT",5,0,5.0,1e-05,20),("CELOUSDT",3,1,5.0,0.001,20),("ARUSDT",3,1,5.0,0.001,20),("KLAYUSDT",4,1,5.0,0.0001,20),("ARPAUSDT",5,0,5.0,1e-05,20),("CTSIUSDT",4,0,5.0,0.0001,20),("LPTUSDT",3,1,5.0,0.001,20),("ENSUSDT",3,1,5.0,0.001,20),("PEOPLEUSDT",5,0,5.0,1e-05,20),("ANTUSDT",3,1,5.0,0.001,20),("ROSEUSDT",5,0,5.0,1e-05,20),("DUSKUSDT",5,0,5.0,1e-05,20),("FLOWUSDT",3,1,5.0,0.001,20),("IMXUSDT",4,0,5.0,0.0001,20),("API3USDT",4,1,5.0,0.001,20),("GMTUSDT",5,0,5.0,0.0001,20),("APEUSDT",4,0,5.0,0.001,20),("BNXUSDT",4,1,5.0,0.01,8),("WOOUSDT",5,0,5.0,1e-05,20),("JASMYUSDT",6,0,5.0,1e-06,20),("DARUSDT",4,1,5.0,0.0001,10),("GALUSDT",5,0,5.0,0.0001,20),("OPUSDT",7,1,5.0,0.0001,20),("INJUSDT",6,1,5.0,0.001,20),("STGUSDT",7,0,5.0,0.0001,20),("FOOTBALLUSDT",5,2,5.0,0.01,20),("SPELLUSDT",7,0,5.0,1e-07,20),("1000LUNCUSDT",7,0,5.0,0.0001,20),("LUNA2USDT",7,0,5.0,0.0001,20),("LDOUSDT",6,0,5.0,0.0001,20),("CVXUSDT",6,0,5.0,0.001,20),("ICPUSDT",6,0,5.0,0.001,20),("APTUSDT",5,1,5.0,0.0001,20),("QNTUSDT",6,1,5.0,0.01,20),("BLUEBIRDUSDT",5,1,5.0,0.001,20);
INSERT INTO variables(name, value) VALUES("available_balance", 0),("balance", 0),("is_order_creation_allowed",  1),("is_program_shutdown_started", 0),("price_stop_percentage", 200);
