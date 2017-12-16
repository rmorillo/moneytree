import math
import os
from colorprinter import ColorPrinter
from tqdm import tqdm
from commontypes import FxMajor
from ncmssignal import CmsSignalProfileReader
from cmstypes import CmsSignalProfileColumns
import pandas as pd

def generate_stats(df, currency_prefix, pip_factor, signal_backtest_period):
    stats = []
    gross_returns_field = currency_prefix + "_gross_returns"
    is_exit_field = currency_prefix + "_is_exit"
    is_stopped_out_field = currency_prefix + "_is_stopped_out"
    stop_loss_hit_type_field = currency_prefix + "_stop_loss_hit_type"
    winning_returns = int(round(df[df[gross_returns_field] > 0][gross_returns_field].sum() * pip_factor))
    losing_returns = int(round(abs(df[df[gross_returns_field] < 0][gross_returns_field].sum()) * pip_factor))
    gross_returns = winning_returns - losing_returns
    returns_win_loss_ratio = 0 if gross_returns == 0 else int((100 * (winning_returns/(winning_returns + losing_returns))))
    stats.append(("Winning Returns", winning_returns, "pips"))
    stats.append(("Losing Returns", losing_returns, "pips"))
    stats.append(("Gross Returns", gross_returns, "pips"))
    stats.append(("Returns Win-Loss Ratio", returns_win_loss_ratio, "%"))
    no_of_wins_result = df[df[gross_returns_field] > 0][gross_returns_field].count()
    no_of_wins = 0 if math.isnan(no_of_wins_result) else no_of_wins_result
    no_of_losses_result = df[df[gross_returns_field] < 0][gross_returns_field].count()
    no_of_losses = 0 if math.isnan(no_of_losses_result) else no_of_losses_result
    no_of_draws = len(df.query("{} & {} == 0".format(is_exit_field,gross_returns_field)))
    total_positions = no_of_wins + no_of_losses
    positions_win_loss_ratio = 0 if total_positions == 0 else int(round((100 * no_of_wins/total_positions)))
    no_of_losing_sl = len(df.query("{} & {} & {} == 1".format(is_exit_field, is_stopped_out_field, stop_loss_hit_type_field)))
    no_of_cutoff_sl = len(df.query("{} & {} & {} == 4".format(is_exit_field, is_stopped_out_field, stop_loss_hit_type_field)))
    no_of_breakeven_sl = len(df.query("{} & {} & {} == 2".format(is_exit_field, is_stopped_out_field, stop_loss_hit_type_field)))
    no_of_winning_sl = len(df.query("{} & {} & {} == 3".format(is_exit_field, is_stopped_out_field, stop_loss_hit_type_field)))
    stats.append(("Total Positions", total_positions, ""))
    stats.append(("Avg. Daily Positions", math.ceil(total_positions/signal_backtest_period), ""))
    stats.append(("# of Wins", no_of_wins, ""))
    stats.append(("# of Losses", no_of_losses, ""))
    stats.append(("# of Draws", no_of_draws, ""))
    stats.append(("# of Losing SL Hits", no_of_losing_sl, ""))
    stats.append(("# of Cutoff SL Hits", no_of_cutoff_sl, ""))
    stats.append(("# of Break-even SL Hits", no_of_breakeven_sl, ""))
    stats.append(("# of Winning SL Hits", no_of_winning_sl, ""))
    stats.append(("Positions Win-Loss Ratio", positions_win_loss_ratio, "%"))
    avg_win_result = df[df[gross_returns_field] > 0][gross_returns_field].mean()
    avg_wins = 0 if math.isnan(avg_win_result) else int(round(avg_win_result * pip_factor))
    avg_loss_result = df[df[gross_returns_field] < 0][gross_returns_field].mean()
    avg_losses = 0 if math.isnan(avg_loss_result) else int(round(abs(avg_loss_result * pip_factor)))
    total_avg_win_loss = avg_wins + avg_losses
    avg_win_loss_ratio = round(100 * (avg_wins/total_avg_win_loss)) if total_avg_win_loss > 0 else 0
    stats.append(("Avg. Win", avg_wins, "pips"))
    stats.append(("Avg. Loss", avg_losses, "pips"))
    stats.append(("Avg. Win-Loss Ratio", avg_win_loss_ratio, "%"))

    max_wins_result = df[df[gross_returns_field] > 0][gross_returns_field].max()
    max_wins = 0 if math.isnan(max_wins_result) else  int(round(max_wins_result * pip_factor))
    max_loss_result = df[df[gross_returns_field] < 0][gross_returns_field].min()
    max_losses = 0 if math.isnan(max_loss_result) else  int(round(abs(max_loss_result * pip_factor)))
    total_max_win_loss = max_wins + max_losses
    max_win_loss_ratio = round(100 * (max_wins/total_max_win_loss)) if total_max_win_loss > 0 else 0

    stats.append(("Max Win", max_wins, "pips"))
    stats.append(("Max Loss", max_losses, "pips"))
    stats.append(("Max Win-Loss Ratio", max_win_loss_ratio, "%"))

    excessive_loss_limit = 20
    excessive_losses = int(abs(df[df[gross_returns_field] < (-excessive_loss_limit/pip_factor)][gross_returns_field].sum()) * pip_factor)
    stats.append(("Excessive Losses (over {} pips)".format(excessive_loss_limit), excessive_losses, "pips"))

    return stats

def print_xy_stats(x_stats, y_stats):
    max_label_width = max([len(item[0]) for item in x_stats]) + 2
    x_max_unit_width = max([len(item[2]) for item in x_stats]) + 1
    x_max_value_width = max([len(format(item[1], ",d")) for item in x_stats]) + 1
    y_max_unit_width = max([len(item[2]) for item in y_stats]) + 1
    y_max_value_width = max([len(format(item[1],",d")) for item in y_stats]) + 1
    for stats_index in range(len(x_stats)):
        x_stat_info = x_stats[stats_index]
        y_stat_info = y_stats[stats_index]
        print((x_stat_info[0] + ":").ljust(max_label_width, " ") + format(x_stat_info[1],",d").rjust(x_max_value_width, " ") + " " + x_stat_info[2].ljust(x_max_unit_width, " ") +
              format(y_stat_info[1], ",d").rjust(y_max_value_width, " ") + " " + y_stat_info[2].ljust(y_max_unit_width, " ")
              )

def validate(df, currency_prefix):
    is_entry_field = currency_prefix + "_is_entry"
    is_exit_field = currency_prefix + "_is_exit"
    total_entry_positions = df[df[is_entry_field]][is_entry_field].count()
    total_exit_positions = df[df[is_exit_field]][is_exit_field].count()
    return total_entry_positions == total_exit_positions

def print_signal_stats(signal_file):
    cmssignal_file_path = signal_file
    base_name_parts = os.path.splitext(os.path.basename(cmssignal_file_path))[0].split("_")
    x_base_currency = base_name_parts[0][:3]
    x_quote_currency = base_name_parts[0][3:]
    y_base_currency = base_name_parts[1][:3]
    y_quote_currency = base_name_parts[1][3:]

    x_currency_pair = FxMajor.get_pair_by_symbols(x_base_currency, x_quote_currency)
    y_currency_pair = FxMajor.get_pair_by_symbols(y_base_currency, y_quote_currency)

    cms_signal_file = CmsSignalProfileReader(cmssignal_file_path)
    cms_signal_file.open()

    signal_rows = []
    for _ in tqdm(range(cms_signal_file.row_count), ncols=70):
        cms_signal_data = cms_signal_file.read()
        if (cms_signal_data is not None):
            signal_rows.append(cms_signal_data)

    df = pd.DataFrame(signal_rows, columns=CmsSignalProfileColumns._fields)
    df["day"] = pd.Series(map(lambda item: item.strftime("%Y%m%d"), df["timestamp"]), index=df.index)

    cprint = ColorPrinter()
    cprint.cfg('c', 'm', 'bux').out('Hello', 'World!')

    signal_backtest_period = df.day.nunique()
    print("Signal Backtest Period:".ljust(40, " ") + "{} days".format(signal_backtest_period))

    #validation
    if validate(df,"x") and validate(df,"y"):
        x_stats = generate_stats(df, "x", x_currency_pair.value.pip_factor, signal_backtest_period)
        y_stats = generate_stats(df, "y", y_currency_pair.value.pip_factor, signal_backtest_period)
        print_xy_stats(x_stats, y_stats)
    else:
        print("Signal validation failed!")

    cms_signal_file.close()