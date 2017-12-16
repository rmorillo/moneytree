import struct
import os

class NFileReader:
    def __init__(self, file_path, row_format):
        self.file_path = file_path
        self.format = row_format
        self.row_size = struct.calcsize(self.format)
        self.file = None
        self.row_count = 0
        
    def open(self):
        self.file = open(self.file_path, 'rb')
        current_position = self.file.tell()
        self.file.seek(0, 2)
        last_position = self.file.tell()
        self.file.seek(current_position)
        self.row_count = int((last_position - current_position) / self.row_size)

    def _read(self):
        byte_data = self.file.read(self.row_size)
        if (len(byte_data) > 0):
            return byte_data
        else:
            return None

    def close(self):
        self.file.close()
