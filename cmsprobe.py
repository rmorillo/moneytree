from adx import AdxIndicator
from rsi import RsiIndicator
from atr import AtrIndicator
from pcorrel import PCorrelIndicator
from cmstypes import *
                                                     
class CmsProbe:
    def __init__(self, probe_config):
        pool_capacity = max(probe_config.lookback_period, probe_config.correlation_period)
        self.struct = CmsProbeStruct()
        self.x_adx = AdxIndicator(pool_capacity, probe_config.lookback_period)
        self.x_rsi = RsiIndicator(pool_capacity, probe_config.lookback_period)
        self.x_atr = AtrIndicator(pool_capacity, probe_config.lookback_period)
        self.y_adx = AdxIndicator(pool_capacity, probe_config.lookback_period)
        self.y_rsi = RsiIndicator(pool_capacity, probe_config.lookback_period)
        self.y_atr = AtrIndicator(pool_capacity, probe_config.lookback_period)
        self.pair_correl = PCorrelIndicator(pool_capacity, probe_config.correlation_period)
        self.is_primed = False

    def price_action(self, timestamp, x_open, x_high, x_low, x_close,
                                   y_open, y_high, y_low, y_close):
        x_adx = self.x_adx.calculate(x_high, x_low, x_close)
        x_rsi = self.x_rsi.calculate(x_close)
        x_atr = self.x_atr.calculate(x_high, x_low, x_close)
        y_adx = self.y_adx.calculate(y_high, y_low, y_close)
        y_rsi = self.y_rsi.calculate(y_close)
        y_atr = self.y_atr.calculate(y_high, y_low, y_close)

        correl = self.pair_correl.calculate(x_close, y_close)
        
        if self.x_adx.is_periodic_dm_primed and self.pair_correl.is_primed:
            return CmsProbeColumns(timestamp, x_open, x_high, x_low, x_close, y_open, y_high, y_low, y_close,
                               x_adx["plus_di"], x_adx["minus_di"], x_adx["adx"],x_rsi["rsi"],x_atr["atr"],
                               y_adx["plus_di"], y_adx["minus_di"], y_adx["adx"],y_rsi["rsi"], y_atr["atr"],
                                correl["coefficient"])
            

        
