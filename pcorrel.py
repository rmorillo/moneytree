from objectpool import ObjectPool
import math

class PCorrelPriceItem:
    def __init__(self): 
        self.update(float('NaN'), float('NaN'), float('NaN'), float('NaN'), float('NaN'))

    def update(self, x_var, y_var, xy, x_squared, y_squared):        
        self.x_var = x_var
        self.y_var = y_var
        self.xy = xy
        self.x_squared = x_squared
        self.y_squared = y_squared        

    def as_dictionary(self):
        values = {"x_var": self.x_var}
        values["y_var"] = self.y_var
        values["xy"] = self.xy
        values["x_squared"] = self.x_squared
        values["y_squared"] = self.y_squared        
        return values

class PCorrelIndicatorItem:
    def __init__(self): 
        self.update(float('NaN'))

    def update(self, coefficient):        
        self.coefficient = coefficient        

    def as_dictionary(self):
        values = {"coefficient": self.coefficient}        
        return values
  
class PCorrelIndicator:
    def __init__(self, capacity, period):        
        self.values = ObjectPool(capacity, lambda: PCorrelIndicatorItem())
        self.period = period
        self.correlation = ObjectPool(period, lambda: PCorrelPriceItem())
        self.is_indicator_primed = False
        self.is_primed = False

    def calculate(self, x_var, y_var):
        xy = x_var * y_var
        x_squared = x_var ** 2
        y_squared = y_var ** 2        
        self.correlation.update(lambda item: item.update(x_var, y_var, xy, x_squared, y_squared))
        
        if (not self.is_indicator_primed):
            self.is_indicator_primed = self.correlation.length >= self.period

        if (self.is_indicator_primed):
            items = list(self.correlation.read_back(self.period, lambda item: item))
            sum_of_x = sum(map(lambda item: item.x_var, items))
            sum_of_y = sum(map(lambda item: item.y_var, items))
            sum_of_xy = sum(map(lambda item: item.xy, items))
            sum_of_x_squared = sum(map(lambda item: item.x_squared, items))
            sum_of_y_squared = sum(map(lambda item: item.y_squared, items))
            r_dividend = (self.period * sum_of_xy) - (sum_of_x * sum_of_y)
            r_divisor_base = ((self.period * sum_of_x_squared) - (sum_of_x ** 2)) * ((self.period * sum_of_y_squared) - (sum_of_y ** 2))

            coefficient = 0

            if self.is_primed:
                coefficient = self.values.current.coefficient
            else:
                self.is_primed = self.values.has_current

            if r_divisor_base > 0:
                r_divisor = math.sqrt(r_divisor_base)
                coefficient = r_dividend/r_divisor

            self.values.update(lambda item: item.update(coefficient))

            return self.values.current.as_dictionary()        

        return None
