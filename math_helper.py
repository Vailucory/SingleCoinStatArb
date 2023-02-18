from sklearn.linear_model import LinearRegression
from binance.helpers import round_step_size
import numpy as np
import math

def static_init(cls):
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls

@static_init
class MathHelper():
    possible_values = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']   
    config=0


    @classmethod
    def static_init(cls):
        pass
        #c = ConfigManager()
        #cls.config = cls.read_config()

    def __init__(self) -> None:
        #ConfigManager.config = self.read_config()
        pass


    @classmethod
    def is_kline_interval_valid(self, kline_interval):
        #return self.possible_values.__contains__(kline_interval)
        pass
    
    @staticmethod
    def is_price_exceed_limit(enter_price, current_price, leverage, limit, order_type):
        if order_type == 'BUY':
            #            200    /  1400 * 20 * 100 = 333.3
            #        1400 - 1200 / 1200 * 20 * 100 = 333.3
            result = (enter_price - current_price) / enter_price * leverage * 100
            return result > limit, result
        # -enter_price + (enter_price * leverage * 100 * result) = current_price
        # result = (enter_price - current_price) 
        #           ----------------------------  * leverage * 100
        #           enter_price 
        # -(result/(leverage * 100) * enter_price) + enter_price = current_price
        # result/(leverage * 100)*enter_price + enter_price = current_price
        if order_type == 'SELL':
            #             200     / 1200 * 20 * 100 = 400
            #       -(1200 - 1400) / 1000 * 20 * 100 = 400
            result = -(enter_price - current_price) / enter_price * leverage * 100
            return result > limit, result

        pass

    @staticmethod
    def calculate_deviation(y):
        y = np.array(y)
        result = np.std(y)
        pass
        return result
    @staticmethod
    def calculate_linear_regression_coefficients(x, y):

        x_normalized = np.array(x).reshape((-1, 1))
        y = np.array(y)

        model = LinearRegression().fit(x_normalized, y)
        y_pred = model.intercept_ + model.coef_ * x
        prediction = []
        for i in y_pred:
            prediction.append(i)
        p1 = [x[0], x[-1]]
        p2 = [prediction[0], prediction[-1]]
        coefficients = np.polyfit(p1, p2, 1)
        return coefficients 

    @staticmethod
    def calculate_polynom(x, coefficients):

        polynomial = np.poly1d(coefficients)
        prediction = polynomial(x)
        pass
        return prediction

    @staticmethod
    def calculate_quantity(total, entry_price, stop_loss_price, precision, minimum_notion):
        #risk = 1.0/100#%
        risk = 0.05/100#%
        max_risk = risk * total
        risk_per_coin = abs(entry_price - stop_loss_price)
        #100
        #5
        quantity = round(max_risk / risk_per_coin, precision)
        #100 * 1.2 = 120
        #5 * 1.2 = 6
        if quantity * stop_loss_price > minimum_notion:
            return quantity

        #1/(10^precision)
        #0 1
        #1 0.1
        #2 0.01
        delta_qnty = 1 / (10**precision)
        while True:
            quantity += delta_qnty
            if quantity * stop_loss_price > minimum_notion:
                return round(quantity, precision)
            pass
            
    @staticmethod
    def calculate_min_reversion_quantity(total, entry_price, precision, minimum_notion, maximum_notion, leverage = 20):
        #TODO

        risk = 2.5/100#%
        
        
        #                       25        
        #50               1000  * 0.025 / 0.5
        quantity = round(total * risk / entry_price * leverage + 1 / (10**precision), precision)
        #100 * 1.2 = 120
        #5 * 1.2 = 6
        if quantity * entry_price > minimum_notion:
            return quantity

        #1/(10^precision)
        #0 1
        #1 0.1
        #2 0.01
        delta_qnty = 1 / (10**precision)
        while True:
            quantity += delta_qnty
            if round(quantity * entry_price, precision)> minimum_notion:
                return round(quantity, precision)
            pass

    @staticmethod
    def calculate_order_profit(enter_price, triggered_price, quantity, order_type):
        if order_type == 'BUY':
            return (triggered_price - enter_price) * quantity
        if order_type == 'SELL':
            return -(triggered_price - enter_price) * quantity 
        

        
    @staticmethod
    def calculate_entering_market_commission(enter_price, quantity):
        maker_commission = 0.0002#0.02%
        return  enter_price * quantity * maker_commission
    @staticmethod
    def calculate_exiting_market_commission(exit_price, quantity):
        taker_commission = 0.0004#0.02%
        return  exit_price * quantity * taker_commission    

    @staticmethod
    def calculate_balance_after_entering_market(balance, enter_price, quantity, leverage):
        maker_commission = 0.0002#0.02%
        #51 =     3.4 * 15
        position = enter_price * quantity 
        #x =    1000 - 51 - 0.0102
        return balance - position / leverage - position * maker_commission

    @staticmethod
    def calculate_order_stop_price(current_price, tick_size, stop_percentage, leverage, side):
        
        delta_price = current_price * stop_percentage / 100 / leverage 
        if side == 'BUY':
             
            return round_step_size(-(stop_percentage/(leverage * 100) * current_price) + current_price, tick_size)
            #return round_step_size(current_price-delta_price, tick_size)
        elif side == 'SELL':
            return round_step_size(stop_percentage/(leverage * 100)*current_price + current_price, tick_size)
        else:
            raise Exception('Invalid order side in calculate_order_stop_price')



        #return round_step_size(price, tick_size)

    @staticmethod
    def round_price(price, tick_size):
        return round_step_size(price, tick_size)

    @staticmethod
    def ceil(number):
        return math.ceil(number)


