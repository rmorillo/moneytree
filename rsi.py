from objectpool import ObjectPool
import pandas as pd

class RsiIndicatorItem:
    def __init__(self): 
        self.update(float('NaN'), float('NaN'), float('NaN'), float('NaN'))

    def update(self, average_gain, average_loss, rs, rsi):        
        self.average_gain = average_gain
        self.average_loss = average_loss
        self.rs = rs
        self.rsi = rsi

    def as_dictionary(self):
        values = {"average_gain": self.average_gain}
        values["average_loss"] = self.average_loss
        values["rs"] = self.rs
        values["rsi"] = self.rsi
        return values

class RsiChangeItem:
    def __init__(self): 
        self.update(float('NaN'), float('NaN'), float('NaN'))
        
    def update(self, price_change, gain, loss):        
        self.price_change = price_change
        self.gain = gain
        self.loss = loss

    def as_dictionary(self):
        values = {"change": self.price_change}
        values["gain"] = self.gain
        values["loss"] = self.loss      
        return values
    
class RsiIndicator:
    def __init__(self, capacity, period):        
        self.values = ObjectPool(capacity, lambda: RsiIndicatorItem())
        self.period = period
        self.price_change = ObjectPool(period, lambda: RsiChangeItem())
        self.previous_price = float('NaN')
        self.current_price = float('NaN')
        self.is_initialized = False
        self.is_price_change_primed = False

    def calculate(self, price):
        if (self.is_initialized):
            self.previous_price = self.current_price
            self.current_price = price
            
            change = self.current_price - self.previous_price
            gain = change if change > 0 else 0
            loss = -change if change < 0 else 0
            self.price_change.update(lambda item: item.update(change, gain, loss))

            if (not self.is_price_change_primed):
                self.is_price_change_primed = self.price_change.length >= self.period

            if (self.is_price_change_primed):
                items = list(self.price_change.read_back(self.period, lambda item: item))
                average_gain = sum(map(lambda item: item.gain, items)) / self.period
                average_loss = sum(map(lambda item: item.loss, items)) / self.period
                rs = 0 if average_loss == 0 else average_gain / average_loss
                rsi = 100 if average_loss == 0 else 100 - (100 / (1 + rs))
                self.values.update(lambda item: item.update(average_gain, average_loss, rs, rsi))

                return self.values.current.as_dictionary()
        else:
            self.current_price = price
            self.is_initialized = True

        return None
