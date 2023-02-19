from numpy import double

def static_init(cls):
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls

@static_init
class IntervalConverter:

    staticmethod  
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

    staticmethod  
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



    pass