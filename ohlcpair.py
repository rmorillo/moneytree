from objectpool import ObjectPool
import pandas as pd

class OhlcPairItem:
    def __init__(self): 
        self.update(None, float('NaN'), float('NaN'), float('NaN'),  float('NaN'),
                    float('NaN'), float('NaN'), float('NaN'), float('NaN'))

    def update(self, timestamp, x_open, x_high, x_low, x_close,
                 y_open, y_high, y_low, y_close):
        self.timestamp = timestamp
        self.x_open = x_open
        self.x_high = x_high
        self.x_low = x_low
        self.x_close = x_close
        self.y_open = y_open
        self.y_high = y_high
        self.y_low = y_low
        self.y_close = y_close

    def as_dictionary(self):
        values = {"timestamp": self.timestamp}
        values["x_open"] = self.x_open
        values["x_high"] = self.x_high
        values["x_low"] = self.x_low
        values["x_close"] = self.x_close
        values["y_open"] = self.y_open
        values["y_high"] = self.y_high
        values["y_low"] = self.y_low
        values["y_close"] = self.y_close
        
        return values
        

class OhlcPair:
    def __init__(self, capacity):
        self.price_bars = ObjectPool(capacity, lambda: OhlcPairItem())

    def price_action(self, timestamp, x_open, x_high, x_low, x_close,
                 y_open, y_high, y_low, y_close):
        self.price_bars.update(lambda item: item.update(timestamp, x_open, x_high, x_low, x_close,
                 y_open, y_high, y_low, y_close))

    def as_dataframe(self, period):
        if (period <= self.price_bars.length):
            return [self.price_bars[i].as_dictionary() for i in range(0,period)]
        else:
            raise Exception("Period is out of range!")
