from commontypes import StopLossHitType

class ProfitAndLossAdvice:
    def __init__(self, is_stopped_out, stop_loss_hit_type):
        self.is_stopped_out = is_stopped_out
        self.stop_loss_hit_type = stop_loss_hit_type

class ProfitAndLossAdvisor:
    def __init__(self, currency_pair_id):
        self.currency_pair_id = currency_pair_id
        self.current_price = float('NaN')
        self.entry_price = float('NaN')
        self.entry_bidask_spread = float('NaN')
        self.position = 0
        self.stop_loss_hit_type = StopLossHitType.Nothing

    def price_action(self, price, atr):
        self.current_price = price
        is_stopped_out = False

        if self.position != 0:
            if self.stop_loss_hit_type == StopLossHitType.Losing:
                running_returns = price - self.entry_price if self.position == 1 else self.entry_price - price
                if running_returns > atr:
                    self.stop_loss_price = self.entry_price if self.entry_price == 1 else self.entry_price - self.entry_bidask_spread
                    self.stop_loss_hit_type = StopLossHitType.BreakEven
                else:
                    self.stop_loss_price = price - self.entry_price - atr if self.position == 1 else self.entry_price - price + atr
            elif self.stop_loss_hit_type == StopLossHitType.BreakEven:
                running_returns = price - self.stop_loss_price if self.position == 1 else self.stop_loss_price - price
                if running_returns > atr * 2:
                    self.stop_loss_price = self.stop_loss_price + atr if self.entry_price == 1 else self.stop_loss_price - atr
                    self.stop_loss_hit_type = StopLossHitType.Winning

            is_stopped_out = price < self.stop_loss_price if self.position == 1 else price > self.stop_loss_price

        return ProfitAndLossAdvice(is_stopped_out, self.stop_loss_hit_type)

    def in_position(self, position, entry_price, entry_bidask_spread):
        self.position = position
        self.entry_price = entry_price
        self.entry_bidask_spread = entry_bidask_spread
        self.stop_loss_hit_type = StopLossHitType.Losing

    def out_of_position(self):
        self.position = 0
        self.entry_price = float('NaN')
        self.entry_bidask_spread = float('NaN')
        self.stop_loss_hit_type = StopLossHitType.Nothing
