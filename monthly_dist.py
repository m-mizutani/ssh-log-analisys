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
        count[msg[b'dt'][:7]] += 1

    csvfd.writerow(['month', 'count'])
    for k, v in sorted(count.items(), key=lambda x: x[0]):
        csvfd.writerow([k.decode('utf'), v])


if __name__ == '__main__': main()
