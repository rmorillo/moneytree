import struct
import math
import datetime as dt
import itertools
from collections import namedtuple
from enum import Enum
from nfiler import NFileReader
from commontypes import CorrelationType, Signal, StructFieldType

class CmsSignalConfig:
    def __init__(self, settings_name, trending_level, momentum_clearance, correlation_offset):
        self.settings_name = settings_name
        self.trending_level= trending_level
        self.momentum_clearance = momentum_clearance
        self.correlation_offset = correlation_offset

class CmsSignalConfigSettings(Enum):
    Harvester = CmsSignalConfig("Harvester", 0, 0, 90)
    Moneytree = CmsSignalConfig("Moneytree", 0, 10, 97)
    Gunslinger = CmsSignalConfig("Gunslinger", 20, 0, 90)
    Marksman = CmsSignalConfig("Marksman", 10, 20, 90)
    Sharpshooter = CmsSignalConfig("Sharpshooter", 30, 30, 0)
    Sniper = CmsSignalConfig("Sniper", 10, 40, 90)

    @staticmethod
    def all_settings():
        return [CmsSignalConfigSettings.Harvester,
                CmsSignalConfigSettings.Moneytree,
                CmsSignalConfigSettings.Gunslinger,
                CmsSignalConfigSettings.Marksman,
                CmsSignalConfigSettings.Sharpshooter,
                CmsSignalConfigSettings.Sniper
                ]
    @staticmethod
    def get_settings_by_name(settings_name):
        for settings in CmsSignalConfigSettings.all_settings():
            if settings.value.settings_name.lower()==settings_name.lower():
                return settings.value
        return None        

class CmsProbeConfig:
    def __init__(self, lookback_period, correlation_period):
        self.lookback_period = lookback_period
        self.correlation_period= correlation_period

class CmsProbeColumnName(Enum):
    Timestamp = "timestamp"
    TimestampDatePart = "ts_date_part"
    TimestampTimePart = "ts_time_part"
    OhlcXOpen = "x_open"
    OhlcXHigh = "x_high"
    OhlcXLow = "x_low"
    OhlcXClose = "x_close"
    OhlcYOpen = "y_open"
    OhlcYHigh = "y_high"
    OhlcYLow = "y_low"
    OhlcYClose = "y_close"
    AdxXPlusDI = "x_plus_di"
    AdxXMinusDI = "x_minus_di"
    AdxX = "x_adx"
    RsiX = "x_rsi"
    AtrX = "x_atr"
    AdxYPlusDI = "y_plus_di"
    AdxYMinusDI = "y_minus_di"
    AdxY = "y_adx"
    RsiY = "y_rsi"
    AtrY = "y_atr"
    Correlation = "correlation"

    @staticmethod
    def struct_fields():
        return [(CmsProbeColumnName.Timestamp, StructFieldType.Integer),
                (CmsProbeColumnName.TimestampDatePart, StructFieldType.Integer),
                (CmsProbeColumnName.TimestampTimePart, StructFieldType.Integer),
                (CmsProbeColumnName.OhlcXOpen, StructFieldType.Float),
                (CmsProbeColumnName.OhlcXHigh, StructFieldType.Float),
                (CmsProbeColumnName.OhlcXLow, StructFieldType.Float),
                (CmsProbeColumnName.OhlcXClose, StructFieldType.Float),
                (CmsProbeColumnName.OhlcYOpen, StructFieldType.Float),
                (CmsProbeColumnName.OhlcYHigh, StructFieldType.Float),
                (CmsProbeColumnName.OhlcYLow, StructFieldType.Float),
                (CmsProbeColumnName.OhlcYClose, StructFieldType.Float),
                (CmsProbeColumnName.AdxXPlusDI, StructFieldType.Float),
                (CmsProbeColumnName.AdxXMinusDI, StructFieldType.Float),
                (CmsProbeColumnName.AdxX, StructFieldType.Float),
                (CmsProbeColumnName.RsiX, StructFieldType.Float),
                (CmsProbeColumnName.AtrX, StructFieldType.Float),
                (CmsProbeColumnName.AdxYPlusDI, StructFieldType.Float),
                (CmsProbeColumnName.AdxYMinusDI, StructFieldType.Float),
                (CmsProbeColumnName.AdxY, StructFieldType.Float),
                (CmsProbeColumnName.RsiY, StructFieldType.Float),
                (CmsProbeColumnName.AtrY, StructFieldType.Float),
                (CmsProbeColumnName.Correlation, StructFieldType.Float)
                ]

    @staticmethod    
    def tuple_names(delimiter=" "):
        return delimiter.join(map(lambda item: item[0].value, filter(lambda item: item[0] != CmsProbeColumnName.TimestampDatePart
                                                and item[0] != CmsProbeColumnName.TimestampTimePart,
                                                      CmsProbeColumnName.struct_fields())))
    
    @staticmethod
    def struct_format():        
        return "".join(map(lambda item: item[1].value, filter(lambda item: item[0] != CmsProbeColumnName.Timestamp,
                                                      CmsProbeColumnName.struct_fields())))
    
CmsProbeColumns = namedtuple("CmsProbeColumns", CmsProbeColumnName.tuple_names())

class CmsProbeStruct:
    def __init__(self):    
        self.struct_format = CmsProbeColumnName.struct_format()        
    def pack(self, tuple_data):
        row_data= []
        timestamp = tuple_data[0]
        row_data.append((timestamp.year * 10000) + (timestamp.month * 100) + timestamp.day)
        row_data.append((timestamp.hour * 10000) + (timestamp.minute * 100) + timestamp.second)
        for i in range(1,len(tuple_data)):
            row_data.append(tuple_data[i])
        
        return struct.pack(self.struct_format, *row_data)
    
    def unpack_tuple(self, byte_data):
        pass

class CmsProbeFileReader(NFileReader):
    def __init__(self, file_path):
        super().__init__(file_path, CmsProbeColumnName.struct_format())        
            
    def read(self):
        byte_data = self._read()
        if (byte_data is None):
            return None
        else:
            row_data = struct.unpack(self.format, byte_data)

            date_part = row_data[0]        
            time_part = row_data[1]        
            timestamp = dt.datetime(math.floor(date_part // 10000),
                                    math.floor((date_part // 100) % 100),
                                    math.floor(date_part % 100),
                                    int((time_part // 10000) % 100),
                                    int((time_part // 100) % 100),
                                    int(time_part % 100))

            x_open = row_data[2]
            x_high = row_data[3]
            x_low = row_data[4]
            x_close = row_data[5]
            y_open = row_data[6]
            y_high = row_data[7]
            y_low = row_data[8]
            y_close = row_data[9]
            x_adx_plus_di = row_data[10]
            x_adx_minus_di = row_data[11]
            x_adx = row_data[12]
            x_rsi = row_data[13]
            x_atr = row_data[14]
            y_adx_plus_di = row_data[15]
            y_adx_minus_di = row_data[16]
            y_adx = row_data[17]
            y_rsi = row_data[18]
            y_atr = row_data[19]
            correl = row_data[20]

            return CmsProbeColumns(timestamp, x_open, x_high, x_low, x_close, y_open, y_high, y_low, y_close,
                                   x_adx_plus_di, x_adx_minus_di, x_adx, x_rsi, x_atr,
                                   y_adx_plus_di, y_adx_minus_di, y_adx, y_rsi, y_atr, correl)
        
class CmsSignalColumnName(Enum):
    Timestamp = "timestamp"
    TimestampDatePart = "ts_date_part"
    TimestampTimePart = "ts_time_part"
    SignalType = "signal_type"
    IsTrending = "is_trending"
    Instrument = "instrument"
    Price = "price"
    Position = "position"
    PreviousPosition = "previous_position"
    PositionStrength = "position_strength"

    @staticmethod
    def struct_fields():
        return [(CmsSignalColumnName.Timestamp, StructFieldType.Integer),
                (CmsSignalColumnName.TimestampDatePart, StructFieldType.Integer),
                (CmsSignalColumnName.TimestampTimePart, StructFieldType.Integer),
                (CmsSignalColumnName.SignalType, StructFieldType.Float),
                (CmsSignalColumnName.IsTrending, StructFieldType.Float),
                (CmsSignalColumnName.Instrument, StructFieldType.Float),
                (CmsSignalColumnName.Price, StructFieldType.Float),
                (CmsSignalColumnName.Position, StructFieldType.Float),
                (CmsSignalColumnName.PreviousPosition, StructFieldType.Float),
                (CmsSignalColumnName.PositionStrength, StructFieldType.Float)
                ]

    @staticmethod
    def tuple_names(delimiter=" "):
        return delimiter.join(
            map(lambda item: item[0].value,
                filter(lambda item: item[0] != CmsSignalColumnName.TimestampDatePart
                                    and item[0] != CmsSignalColumnName.TimestampTimePart,
                       CmsSignalColumnName.struct_fields())))

    @staticmethod
    def struct_format():
        return "".join(map(lambda item: item[1].value,
                           filter(lambda item: item[0] != CmsSignalColumnName.Timestamp,
                                  CmsSignalColumnName.struct_fields())))

CmsSignalColumns = namedtuple("CmsSignalColumns", CmsSignalColumnName.tuple_names())

class CmsSignalStruct:
    def __init__(self):
        self.struct_format = CmsSignalColumnName.struct_format()

    def pack(self, tuple_data):
        row_data = []
        timestamp = tuple_data[0]
        row_data.append((timestamp.year * 10000) + (timestamp.month * 100) + timestamp.day)
        row_data.append((timestamp.hour * 10000) + (timestamp.minute * 100) + timestamp.second)
        for i in range(1, len(tuple_data)):
            row_data.append(tuple_data[i])

        return struct.pack(self.struct_format, *row_data)

    def unpack_tuple(self, byte_data):
        pass

class CmsSignalProfileColumnName(Enum):
    Timestamp = "timestamp"
    TimestampDatePart = "ts_date_part"
    TimestampTimePart = "ts_time_part"
    ClosingPrice = "closing_price"
    CorrelationStatus = "correlation_status"
    IsTrending = "is_trending"
    TrendDirection = "trend_direction"
    PriceMovement = "price_movement"
    DirectionalIndex = "directional_index"
    CorrelationStrength = "correlation_strength"
    DirectionalMomentum = "directional_momentum"
    InPosition = "in_position"
    DirectionalView = "directional_view"
    DirectionalPositionCounter = "directional_position_counter"
    PositionStrengthIndex = "position_strength_index"
    IsEntry = "is_entry"
    EntryPosition = "entry_position"
    EntryPrice = "entry_price"
    EntryPositionStrengthIndex = "entry_position_strength_index"
    IsExit = "is_exit"
    ExitPosition = "exit_position"
    ExitPrice = "exit_price"
    IsReversal = "is_reversal"
    GrossReturns = "gross_returns"
    IsStoppedOut = "is_stopped_out"
    StopLossPrice = "stop_loss_price"
    StopLossHitType = "stop_loss_hit_type"

    @staticmethod
    def correlated_trend_fields():
        return [(CmsSignalProfileColumnName.ClosingPrice, StructFieldType.Float),
                (CmsSignalProfileColumnName.IsTrending, StructFieldType.Boolean),
                (CmsSignalProfileColumnName.TrendDirection, StructFieldType.Integer),
                (CmsSignalProfileColumnName.PriceMovement, StructFieldType.Integer),
                (CmsSignalProfileColumnName.DirectionalIndex, StructFieldType.Float),
                (CmsSignalProfileColumnName.CorrelationStrength, StructFieldType.Float),
                (CmsSignalProfileColumnName.DirectionalMomentum, StructFieldType.Integer)
                ]

    @staticmethod
    def position_fields():
        return [(CmsSignalProfileColumnName.InPosition, StructFieldType.Boolean),
                (CmsSignalProfileColumnName.DirectionalView, StructFieldType.Integer),
                (CmsSignalProfileColumnName.DirectionalPositionCounter, StructFieldType.Integer),
                (CmsSignalProfileColumnName.PositionStrengthIndex, StructFieldType.Integer),
                (CmsSignalProfileColumnName.IsEntry, StructFieldType.Boolean),
                (CmsSignalProfileColumnName.EntryPosition, StructFieldType.Integer),
                (CmsSignalProfileColumnName.EntryPrice, StructFieldType.Float),
                (CmsSignalProfileColumnName.EntryPositionStrengthIndex, StructFieldType.Integer),
                (CmsSignalProfileColumnName.IsExit, StructFieldType.Boolean),
                (CmsSignalProfileColumnName.ExitPosition, StructFieldType.Integer),
                (CmsSignalProfileColumnName.ExitPrice, StructFieldType.Float),
                (CmsSignalProfileColumnName.IsReversal, StructFieldType.Boolean),
                (CmsSignalProfileColumnName.GrossReturns, StructFieldType.Float),
                (CmsSignalProfileColumnName.IsStoppedOut, StructFieldType.Boolean),
                (CmsSignalProfileColumnName.StopLossPrice, StructFieldType.Float),
                (CmsSignalProfileColumnName.StopLossHitType, StructFieldType.Integer)
                 ]

    @staticmethod
    def timestamp_field():
        return [(CmsSignalProfileColumnName.Timestamp, StructFieldType.Integer)]

    @staticmethod
    def correlation_status_field():
        return [(CmsSignalProfileColumnName.CorrelationStatus, StructFieldType.Integer)]

    @staticmethod
    def timestamp_split_fields():
        return [(CmsSignalProfileColumnName.TimestampDatePart, StructFieldType.Integer),
         (CmsSignalProfileColumnName.TimestampTimePart, StructFieldType.Integer)]

    @staticmethod
    def signal_profile_tuple_names(delimiter=" "):
        return delimiter.join(itertools.chain(map(lambda item: item[0].value,
                                                  CmsSignalProfileColumnName.timestamp_field()),
                                              map(lambda item: item[0].value,
                                                  CmsSignalProfileColumnName.correlation_status_field()),
                                              map(lambda item: "x_" + item[0].value,
                                                  CmsSignalProfileColumnName.correlated_trend_fields()),
                                              map(lambda item: "x_" + item[0].value,
                                                  CmsSignalProfileColumnName.position_fields()),
                                              map(lambda item: "y_" + item[0].value,
                                                  CmsSignalProfileColumnName.correlated_trend_fields()),
                                              map(lambda item: "y_" + item[0].value,
                                                  CmsSignalProfileColumnName.position_fields())))

    @staticmethod
    def position_tuple_names(delimiter=" "):
        return delimiter.join(map(lambda item: item[0].value,
                                                  CmsSignalProfileColumnName.position_fields()))


    @staticmethod
    def struct_format():
        return "".join(itertools.chain(map(lambda item: item[1].value,
                                                  CmsSignalProfileColumnName.timestamp_split_fields()),
                                              map(lambda item: item[1].value,
                                                  CmsSignalProfileColumnName.correlation_status_field()),
                                              map(lambda item: item[1].value,
                                                  CmsSignalProfileColumnName.correlated_trend_fields()),
                                              map(lambda item: item[1].value,
                                                  CmsSignalProfileColumnName.position_fields()),
                                              map(lambda item: item[1].value,
                                                  CmsSignalProfileColumnName.correlated_trend_fields()),
                                              map(lambda item: item[1].value,
                                                  CmsSignalProfileColumnName.position_fields())))

class CmsSignalProfileStruct:
    def __init__(self):
        self.struct_format = CmsSignalProfileColumnName.struct_format()

    def pack(self, tuple_data):
        row_data = []
        timestamp = tuple_data[0]
        row_data.append((timestamp.year * 10000) + (timestamp.month * 100) + timestamp.day)
        row_data.append((timestamp.hour * 10000) + (timestamp.minute * 100) + timestamp.second)
        for i in range(1, len(tuple_data)):
            row_data.append(tuple_data[i])

        return struct.pack(self.struct_format, *row_data)

    def unpack_tuple(self, byte_data):
        pass

CmsSignalProfileColumns = namedtuple("CmsProbeColumns", CmsSignalProfileColumnName.signal_profile_tuple_names())

CorrelatedTrendColumns = namedtuple("CorrelatedTrendColumns", "price is_trending trend_direction price_movement directional_index")

CmsSignalColumns = namedtuple("CmsSignalColumns", "signal, signal_profile, probe_data")

CmsPositionColumns = namedtuple("CmsPositionColumns", CmsSignalProfileColumnName.position_tuple_names())

CorrelatedMarketViewColumns = namedtuple("CorrelatedMarketViewColumns", "signal_activated x_market_view y_market_view correlation_state is_normally_correlated_view")

class CmsSignalProfileReader(NFileReader):
    def __init__(self, file_path):
        super().__init__(file_path, CmsSignalProfileColumnName.struct_format())

    def read(self):
        byte_data = self._read()
        if (byte_data is None):
            return None
        else:
            row_data = struct.unpack(self.format, byte_data)

            date_part = row_data[0]
            time_part = row_data[1]
            timestamp = dt.datetime(math.floor(date_part // 10000),
                                    math.floor((date_part // 100) % 100),
                                    math.floor(date_part % 100),
                                    int((time_part // 10000) % 100),
                                    int((time_part // 100) % 100),
                                    int(time_part % 100))

            return CmsSignalProfileColumns(timestamp, *row_data[2:])        
