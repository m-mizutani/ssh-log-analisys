#!/usr/bin/env python

import sys
import msgpack
import csv
import geoip2.database
import os

from collections import defaultdict

def main():
    if len(sys.argv) != 3:
        print('usage) {0} <in_file> <out_csv>'.format(sys.argv[0]))
        exit(1)
    mmdb = os.environ.get('GEOIP_MMDB') or '/usr/local/var/GeoIP/GeoLite2-City.mmdb'
    reader = geoip2.database.Reader(mmdb)
    count = defaultdict(int)
    csvfd = csv.writer(open(sys.argv[2], 'w'))

    cache = {}
    for msg in msgpack.Unpacker(open(sys.argv[1], 'rb')):
        addr = msg[b'remote'].decode('utf')
        if addr in cache:
            count[cache[addr]] += 1
            continue
        
        try:
            geo = reader.city(addr)
            count[geo.country.name] += 1
            cache[addr] = geo.country.name
        except geoip2.errors.AddressNotFoundError as e:
            print('not found:', addr)

        
    csvfd.writerow(['country', 'count'])
    for k, v in sorted(count.items(), key=lambda x: x[1], reverse=True):
        csvfd.writerow([k, v])


if __name__ == '__main__': main()
