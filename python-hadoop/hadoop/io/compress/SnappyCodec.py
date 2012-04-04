#!/usr/bin/env python

import snappy 

from hadoop.io.InputStream import DataInputBuffer

class SnappyCodec:
    def compress(self, data):
        return snappy.compress(data)

    def decompress(self, data):
        return snappy.decompress(data)

    def decompressInputStream(self, data):
        print "the raw data is: %s" % data
        return DataInputBuffer(snappy.decompress(data))

DefaultCodec = SnappyCodec

