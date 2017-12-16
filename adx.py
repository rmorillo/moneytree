from objectpool import ObjectPool
import pandas as pd

class AdxIndicatorItem:
    def __init__(self): 
        self.update(float('NaN'), float('NaN'), float('NaN'))

    def update(self, plus_di, minus_di, adx):        
        self.plus_di = plus_di
        self.minus_di = minus_di
        self.adx = adx        

    def as_dictionary(self):
        values = {"plus_di": self.plus_di}
        values["minus_di"] = self.minus_di
        values["adx"] = self.adx        
        return values

class AdxDmItem:
    def __init__(self): 
        self.update(float('NaN'), float('NaN'), float('NaN'))

    def update(self, true_range, plus_dm, minus_dm):        
        self.true_range = true_range
        self.plus_dm = plus_dm
        self.minus_dm = minus_dm

    def as_dictionary(self):
        values = {"true_range": self.true_range}
        values["plus_dm"] = self.plus_dm
        values["minus_dm"] = self.minus_dm
        
        return values

class AdxPeriodicDmItem:
    def __init__(self): 
        self.update(float('NaN'), float('NaN'), float('NaN'),float('NaN'), float('NaN'), float('NaN'),
                    float('NaN'), float('NaN'))

    def update(self, periodic_true_range, periodic_plus_dm, periodic_minus_dm, periodic_plus_di, periodic_minus_di,
               di_diff, di_sum, dx):
        self.periodic_true_range = periodic_true_range
        self.periodic_plus_dm = periodic_plus_dm
        self.periodic_minus_dm = periodic_minus_dm
        self.periodic_plus_di = periodic_plus_di
        self.periodic_minus_di = periodic_minus_di
        self.di_diff = di_diff
        self.di_sum = di_sum
        self.dx = dx

    def as_dictionary(self):
        values = {"periodic_true_range": self.periodic_true_range}
        values["periodic_plus_dm"] = self.periodic_plus_dm
        values["periodic_minus_dm"] = self.periodic_minus_dm
        values["periodic_plus_di"] = self.periodic_plus_di
        values["periodic_minus_di"] = self.periodic_minus_di
        values["di_diff"] = self.di_diff
        values["di_sum"] = self.di_sum
        values["dx"] = self.dx            
        return values    

class AdxIndicator:
    def __init__(self, capacity, period):        
        self.values = ObjectPool(capacity, lambda: AdxIndicatorItem())
        self.period = period        
        self.previous_high = float('NaN')
        self.previous_low = float('NaN')
        self.previous_close = float('NaN')
        self.current_high = float('NaN')
        self.current_low = float('NaN')
        self.current_close = float('NaN')
        self.dm = ObjectPool(period, lambda: AdxDmItem())
        self.periodic_dm = ObjectPool(period, lambda: AdxPeriodicDmItem())        
        self.is_primed = False
        self.is_dm_primed = False
        self.is_periodic_dm_initialized= False
        self.is_periodic_dm_primed= False        
        self.is_initialized = False
        self.are_values_initialized = False

    def calculate(self, high, low, close):
        if (self.is_initialized):
            self.previous_high = self.current_high
            self.previous_low = self.current_low
            self.previous_close = self.current_close
            self.current_high = high
            self.current_low = low
            self.current_close = close

            #calculate Directional Movement
            true_range = max(self.current_high - self.current_low, abs(self.current_high - self.previous_close),
                             abs(self.current_low - self.previous_close))
            plus_high_diff = self.current_high - self.previous_high
            plus_low_diff = self.previous_low - self.current_low 
            plus_dm = max(plus_high_diff, 0) if plus_high_diff > plus_low_diff else 0
            minus_low_diff = self.previous_low - self.current_low
            minus_high_diff = self.current_high - self.previous_high
            minus_dm = max(minus_low_diff, 0) if minus_low_diff > minus_high_diff else 0
            self.dm.update(lambda item: item.update(true_range, plus_dm, minus_dm))
            
            if (not self.is_dm_primed):
                self.is_dm_primed = self.dm.length >= self.period

            if (self.is_dm_primed):
                periodic_true_range = float('NaN')
                periodic_plus_dm = float('NaN')
                periodic_minus_dm = float('NaN')
                if (self.is_periodic_dm_initialized):
                    periodic_true_range = self.periodic_dm.current.periodic_true_range - (self.periodic_dm.current.periodic_true_range/self.period) + self.dm.current.true_range
                    periodic_plus_dm =  self.periodic_dm.current.periodic_plus_dm - (self.periodic_dm.current.periodic_plus_dm/self.period) + self.dm.current.plus_dm
                    periodic_minus_dm =  self.periodic_dm.current.periodic_minus_dm - (self.periodic_dm.current.periodic_minus_dm/self.period) + self.dm.current.minus_dm
                else:
                    items = list(self.dm.read_back(self.period, lambda item: item))
                    periodic_true_range = sum(map(lambda item: item.true_range, items))
                    periodic_plus_dm = sum(map(lambda item: item.plus_dm, items))
                    periodic_minus_dm = sum(map(lambda item: item.minus_dm, items))
                    self.is_periodic_dm_initialized = True

                periodic_plus_di = 100 * (periodic_plus_dm/periodic_true_range)
                periodic_minus_di = 100 * (periodic_minus_dm/periodic_true_range)
                di_diff = abs(periodic_plus_di - periodic_minus_di)
                di_sum = periodic_plus_di + periodic_minus_di
                dx = 0 if di_sum == 0 else 100 * (di_diff/di_sum)
                
                self.periodic_dm.update(lambda item: item.update(periodic_true_range, periodic_plus_dm, periodic_minus_dm,
                                                                 periodic_plus_di, periodic_minus_di, di_diff, di_sum, dx))

                if (not self.is_periodic_dm_primed):
                    self.is_periodic_dm_primed = self.periodic_dm.length >= self.period

                if (self.is_periodic_dm_primed):
                    adx = float('NaN')
                    if (self.are_values_initialized):
                        adx = ((self.values.current.adx * (self.period - 1)) + self.periodic_dm.current.dx) / self.period
                    else:
                        adx = sum(map(lambda item: item.dx, self.periodic_dm.read_back(self.period, lambda item: item)))/self.period
                        self.are_values_initialized = True

                    self.values.update(lambda item: item.update(self.periodic_dm.current.periodic_plus_di, self.periodic_dm.current.periodic_minus_di,
                                                                adx))
                    return self.values.current.as_dictionary()
        else:
            self.current_high = high
            self.current_low = low
            self.current_close = close
            self.is_initialized = True

        return None
            
            
            
        
    
