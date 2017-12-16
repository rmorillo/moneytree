import math
import datetime as dt
import struct
from enum import Enum
from collections import namedtuple
from nfiler import NFileReader
from commontypes import StructFieldType

class OhlcPairFeedColumnName(Enum):
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

    @staticmethod
    def struct_fields():
        return [(OhlcPairFeedColumnName.Timestamp, StructFieldType.Void),
                (OhlcPairFeedColumnName.TimestampDatePart, StructFieldType.Integer),
                (OhlcPairFeedColumnName.TimestampTimePart, StructFieldType.Integer),
                (OhlcPairFeedColumnName.OhlcXOpen, StructFieldType.Float),
                (OhlcPairFeedColumnName.OhlcXHigh, StructFieldType.Float),
                (OhlcPairFeedColumnName.OhlcXLow, StructFieldType.Float),
                (OhlcPairFeedColumnName.OhlcXClose, StructFieldType.Float),
                (OhlcPairFeedColumnName.OhlcYOpen, StructFieldType.Float),
                (OhlcPairFeedColumnName.OhlcYHigh, StructFieldType.Float),
                (OhlcPairFeedColumnName.OhlcYLow, StructFieldType.Float),
                (OhlcPairFeedColumnName.OhlcYClose, StructFieldType.Float)]

    @staticmethod    
    def tuple_names():
        return " ".join(map(lambda item: item[0].value, filter(lambda item: item[0] != OhlcPairFeedColumnName.TimestampDatePart
                                                and item[0] != OhlcPairFeedColumnName.TimestampTimePart,
                                                      OhlcPairFeedColumnName.struct_fields())))
    
    @staticmethod
    def struct_format():        
        return "".join(map(lambda item: item[1].value, filter(lambda item: item[0] != OhlcPairFeedColumnName.Timestamp,
                                                      OhlcPairFeedColumnName.struct_fields())))
    
OhlcPairFeedColumns = namedtuple("OhlcPairFeedColumns", OhlcPairFeedColumnName.tuple_names())

class OhlcPairFeedFileReader(NFileReader):
    def __init__(self, file_path):
        super().__init__(file_path, OhlcPairFeedColumnName.struct_format())
            
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

            return OhlcPairFeedColumns(timestamp, x_open, x_high, x_low, x_close, y_open, y_high, y_low, y_close)
            
