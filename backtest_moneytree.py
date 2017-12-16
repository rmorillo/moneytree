import sys
import os
from tqdm import tqdm
from commontypes import CorrelationType, CorrelationMode, FxCorrelatedPair
from ohlcfeed import OhlcPairFeedFileReader
from acmebroker import AcmeBroker
from ohlcpair import OhlcPair
from atr import AtrIndicator
from collections import namedtuple
import pandas as pd

acme = AcmeBroker("feeds")
correlated_pair_name = sys.argv[1]
timeframe = sys.argv[2]
month_year_start = int(sys.argv[3].replace("-",""))

feed = acme.get_feed(correlated_pair_name, timeframe)
ohlcfeed_file_path = os.path.join(acme.feeds_folder, feed[3])
ohlcfeed_file = OhlcPairFeedFileReader(ohlcfeed_file_path)

correlated_pair_id = feed[1].value.correlated_pair_id
correlated_pair = FxCorrelatedPair.get_correlated_pair_by_id(correlated_pair_id).value

correlation_type = correlated_pair.correlation_type
ohlcfeed_file.open()

row_count = ohlcfeed_file.row_count

ohlcfeed_data = ohlcfeed_file.read()
while (ohlcfeed_data.timestamp.year * 100) + ohlcfeed_data.timestamp.month < month_year_start:
    ohlcfeed_data = ohlcfeed_file.read()
    row_count -= 1

#Probes
ohlc_pair = OhlcPair(14)
x_atr96 = AtrIndicator(96, 96)
y_atr96 = AtrIndicator(96, 96)

x_pip_move = float('NaN')
y_pip_move = float('NaN')

x_pip_range = float('NaN')
y_pip_range = float('NaN')


x_direction = 0
y_direction = 0

x_avg_pip_spread = 2
y_avg_pip_spread = 2

x_pip_factor = correlated_pair.x_currency_pair.value.pip_factor
y_pip_factor = correlated_pair.y_currency_pair.value.pip_factor

previous_correlation_state = CorrelationMode.Unknown

is_primed = False
is_backfilled = False

high_volatility_factor = 6
x_in_position = False
y_in_position = False

x_win_counter = 0
x_draw_counter = 0
x_loss_counter = 0

y_win_counter = 0
y_draw_counter = 0
y_loss_counter = 0

x_position = 0
y_position = 0

x_cum_gross_returns = 0
y_cum_gross_returns = 0

x_position_size = 0
y_position_size = 0

x_position_sized_cum_gross_returns = 0
y_position_sized_cum_gross_returns = 0

position_size_cap = 10

x_cum_net_returns = 0
y_cum_net_returns = 0

x_cum_position_sized_net_returns = 0
y_cum_position_sized_net_returns = 0

signal_data = []
signal_row = None

SignalDataColumns = namedtuple("SignalDataColumns", "timestamp in_position is_entry entry_position entry_price entry_spread entry_position_size is_exit exit_price")

for row_index in tqdm(range(row_count), ncols=70):
    ohlcfeed_data = ohlcfeed_file.read()

    if (ohlcfeed_data is not None):
        ohlc_pair.price_action(ohlcfeed_data.timestamp, ohlcfeed_data.x_open, ohlcfeed_data.x_high,
                                              ohlcfeed_data.x_low, ohlcfeed_data.x_close,
                                              ohlcfeed_data.y_open, ohlcfeed_data.y_high, ohlcfeed_data.y_low,
                                              ohlcfeed_data.y_close)

        x_current_atr = x_atr96.calculate(ohlcfeed_data.x_high, ohlcfeed_data.x_low, ohlcfeed_data.x_close)
        y_current_atr = y_atr96.calculate(ohlcfeed_data.y_high, ohlcfeed_data.y_low, ohlcfeed_data.y_close)

        if is_primed:
            x_pip_move = (ohlc_pair.price_bars.current.x_close - ohlc_pair.price_bars.previous.x_close) * x_pip_factor
            y_pip_move = (ohlc_pair.price_bars.current.y_close - ohlc_pair.price_bars.previous.y_close) * y_pip_factor

            x_pip_range = (ohlcfeed_data.x_high - ohlcfeed_data.x_low) * x_pip_factor
            y_pip_range = (ohlcfeed_data.y_high - ohlcfeed_data.y_low) * y_pip_factor

            if x_pip_move > 0:
                x_direction = 1
            elif x_pip_move < 0:
                x_direction = -1
            else:
                x_direction = 0

            if y_pip_move > 0:
                y_direction = 1
            elif y_pip_move < 0:
                y_direction = -1
            else:
                y_direction = 0

            correlation_state = CorrelationMode.Unknown

            if correlation_type == CorrelationType.Negative:
                if x_direction != y_direction:
                    correlation_state = CorrelationMode.Normal
                else:
                    correlation_state = CorrelationMode.Abnormal
            elif correlation_type == CorrelationType.Positive:
                if x_direction == y_direction:
                    correlation_state = CorrelationMode.Normal
                else:
                    correlation_state = CorrelationMode.Abnormal

            if is_backfilled:
                has_signal = previous_correlation_state == CorrelationMode.Normal and correlation_state == CorrelationMode.Abnormal

                if x_in_position:
                    if x_direction == 0:
                        x_draw_counter += 1
                    elif x_direction == x_position:
                        x_win_counter += 1
                    else:
                        x_loss_counter += 1

                    x_gross_returns = x_position * x_pip_move
                    x_cum_gross_returns += x_gross_returns
                    x_position_sized_gross_returns = x_gross_returns * x_position_size
                    x_position_sized_cum_gross_returns += x_position_sized_gross_returns

                    x_net_returns = x_gross_returns + (0 if x_position ==1 else -x_avg_pip_spread)
                    x_cum_net_returns += x_net_returns
                    x_cum_position_sized_net_returns += x_net_returns * x_position_size

                    x_in_position = False

                    signal_row = SignalDataColumns(ohlcfeed_data.timestamp, False, False, signal_row.entry_position,
                                                   signal_row.entry_price, signal_row.entry_spread, signal_row.entry_position_size, True, ohlcfeed_data.x_close)
                elif has_signal:
                    x_in_position = x_pip_range > (x_atr96.values.current.atr * x_pip_factor * high_volatility_factor)
                    if x_in_position:
                        x_position = -x_direction
                        x_position_size = ((x_pip_range / (x_atr96.values.current.atr * x_pip_factor)) - high_volatility_factor) + 1
                        if x_position_size > position_size_cap:
                            x_position_size = position_size_cap

                        signal_row = SignalDataColumns(ohlcfeed_data.timestamp, True, True, x_position,
                                                       ohlcfeed_data.x_close, x_avg_pip_spread, x_position_size, False, 0)
                else:
                    signal_row = SignalDataColumns(ohlcfeed_data.timestamp, False, False, 0, 0, 0, 0, False, 0)

                signal_data.append(signal_row)

                previous_correlation_state = correlation_state

            else:
                is_backfilled = x_atr96.values.has_current and y_atr96.values.has_current
        else:
            is_primed = ohlc_pair.price_bars.has_previous


print("Win: {}".format(x_win_counter))
print("Loss: {}".format(x_loss_counter))
print("Draw: {}".format(x_draw_counter))
print("Win/Loss Ratio: {}%".format(round(x_win_counter/(x_loss_counter+x_win_counter) * 100)))
print("Gross Returns: {}".format(round(x_cum_gross_returns)))
print("Position Sized Gross Returns: {}".format(round(x_position_sized_cum_gross_returns)))
print("Net Returns: {}".format(round(x_cum_net_returns)))
print("Position Sized Net Returns: {}".format(round(x_cum_position_sized_net_returns)))

df = pd.DataFrame(signal_data, columns=SignalDataColumns._fields)

def calc_gross_returns(df, index):
    if df["is_exit"][index]:
        if df["entry_position"][index] == -1:
            return df["entry_price"][index] - df["exit_price"][index]
        elif df["entry_position"][index] == 1:
            return df["exit_price"][index] - df["entry_price"][index]
        else:
            return 0
    else:
        return 0

def calc_net_returns(df, index):
    if df["is_exit"][index]:
        if df["entry_position"][index] == -1:
            return (df["entry_price"][index] - (df["entry_spread"][index]/x_pip_factor)) - df["exit_price"][index]
        elif df["entry_position"][index] == 1:
            return df["exit_price"][index] - df["entry_price"][index]
        else:
            return 0
    else:
        return 0

def calc_position_sized_returns(df, index):
    if df["is_exit"][index]:
        return  df["net_returns"][index] * df["entry_position_size"][index]
    else:
        return 0

def cumulative_sum(cum_sum, value):
    cum_sum["last_value"] += value
    return cum_sum["last_value"]

df["gross_returns"] = pd.Series(map(lambda index: calc_gross_returns(df, index), df.index), index=df.index)
df["net_returns"] = pd.Series(map(lambda index: calc_net_returns(df, index), df.index), index=df.index)
df["position_sized_returns"] = pd.Series(map(lambda index: calc_position_sized_returns(df, index), df.index), index=df.index)

cum_sum = {"last_value": 0}
df["cum_gross_returns"] = pd.Series(map(lambda item: cumulative_sum(cum_sum, item), df["gross_returns"]), index=df.index)

cum_sum["last_value"] = 0
df["cum_net_returns"] = pd.Series(map(lambda item: cumulative_sum(cum_sum, item), df["net_returns"]), index=df.index)

cum_sum["last_value"] = 0
df["cum_position_sized_returns"] = pd.Series(map(lambda item: cumulative_sum(cum_sum, item), df["position_sized_returns"]), index=df.index)

csv_filename = "{}.csv".format(correlated_pair_name)
df.to_csv(csv_filename)
ohlcfeed_file.close()
