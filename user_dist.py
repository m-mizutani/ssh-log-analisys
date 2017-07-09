#!/usr/bin/env python

import sys
import msgpack
import csv
from collections import defaultdict

def main():
    if len(sys.argv) != 3:
        print('usage) {0} <in_file> <out_csv>'.format(sys.argv[0]))
        exit(1)
    
    count = defaultdict(int)
    csvfd = csv.writer(open(sys.argv[2], 'w'))
    
    for msg in msgpack.Unpacker(open(sys.argv[1], 'rb')):
        username = msg[b'user'].decode('utf8')
        count[username] += 1

    csvfd.writerow(['username', 'count'])
    for k, v in sorted(count.items(), key=lambda x: x[1], reverse=True):
        csvfd.writerow([k, v])


if __name__ == '__main__': main()
