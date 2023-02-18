import json
from logging import exception

def static_init(cls):
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls

@static_init
class ConfigManager():
    possible_values = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']   
    config=0


    @classmethod
    def static_init(cls):
        #c = ConfigManager()
        cls.config = cls.read_config()

    def __init__(self) -> None:
        ConfigManager.config = self.read_config()
        pass

    @classmethod
    def is_kline_interval_valid(self, kline_interval):
        return self.possible_values.__contains__(kline_interval)
   
    @classmethod
    def read_config(self):
        json_content = open('config.json', 'r').read().strip()
        decoder = json.JSONDecoder()
        config = decoder.decode(json_content)

        if not self.is_kline_interval_valid(config['kline_interval']):
            raise Exception('Invalid kline_interval value\nPossible values:\n' + '\n'.join(self.possible_values))
        if config['bar_count'] < 2:
            raise Exception('Minimum 2 bars required\nYour value: {0}'.format(str(config['bar_count'])))
            
        return config
