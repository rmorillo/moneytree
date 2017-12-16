from objectpool import ObjectPool

class AtrIndicatorItem:
    def __init__(self): 
        self.update(float('NaN'))

    def update(self, atr):
        self.atr = atr        

    def as_dictionary(self):
        values = {"atr": self.atr}
        return values

class TrItem:
    def __init__(self): 
        self.update(float('NaN'))

    def update(self, true_range):
        self.true_range = true_range

    def as_dictionary(self):
        values = {"true_range": self.true_range}
        
        return values


class AtrIndicator:
    def __init__(self, capacity, period):        
        self.values = ObjectPool(capacity, lambda: AtrIndicatorItem())
        self.period = period        
        self.previous_high = float('NaN')
        self.previous_low = float('NaN')
        self.previous_close = float('NaN')
        self.current_high = float('NaN')
        self.current_low = float('NaN')
        self.current_close = float('NaN')
        self.tr = ObjectPool(period, lambda: TrItem())
        self.is_primed = False
        self.is_tr_primed = False
        self.is_initialized = False

    def calculate(self, high, low, close):
        if (self.is_initialized):
            self.previous_high = self.current_high
            self.previous_low = self.current_low
            self.previous_close = self.current_close
            self.current_high = high
            self.current_low = low
            self.current_close = close

            true_range = max(self.current_high - self.current_low, abs(self.current_high - self.previous_close),
                             abs(self.current_low - self.previous_close))

            self.tr.update(lambda item: item.update(true_range))

            if (not self.is_tr_primed):
                self.is_tr_primed = self.tr.length >= self.period

            if (self.is_tr_primed):
                atr = sum(self.tr.read_back(self.period, lambda item: item.true_range)) / self.period
                self.values.update(lambda item: item.update(atr))
                return self.values.current.as_dictionary()
        else:
            self.current_high = high
            self.current_low = low
            self.current_close = close
            self.is_initialized = True

        return None
            
            
            
        
    
