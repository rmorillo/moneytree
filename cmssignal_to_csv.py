import sys
import struct
import pytz
import math
import datetime as dt
import os
from tqdm import tqdm
import datetime as dt
from ncmssignal import CmsSignalProfileReader, CmsSignalProfileColumnName

cmssignal_file_path = sys.argv[1]

cms_signal_file = CmsSignalProfileReader(cmssignal_file_path)
cms_signal_file.open()

base_file_name = os.path.basename(cmssignal_file_path)
csv_file_path = base_file_name + ".csv"
csv_file = open(csv_file_path, "w")
csv_header = CmsSignalProfileColumnName.signal_profile_tuple_names(",")
csv_file.write("{}\n".format(csv_header))
for row_index in tqdm(range(cms_signal_file.row_count), ncols=70):
    cms_signal_data = cms_signal_file.read()
    if (cms_signal_data is not None):
        cms_signal_string_list = map(lambda item: item.strftime("%Y%m%d %H:%M:%S") if type(item) is dt.datetime else str(item), cms_signal_data)
        csv_line = ",".join(cms_signal_string_list)
        csv_file.write("{}\n".format(csv_line))

cms_signal_file.close()
csv_file.close()
