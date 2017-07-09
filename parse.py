#!/usr/bin/env python

import sys
import os
import msgpack
import gzip
import tempfile
import datetime
import time
import re

def filter_logs(in_fd, out_fd):
    KEYWORD = ': Failed password for'
    buf = []
    for line in in_fd:
        if KEYWORD in line:
            buf.append(line)

    for line in reversed(buf):
        out_fd.write(line)

    return len(buf)
        
def dump_ssh_log(in_dir, fo):
    BASENAME = 'auth.log'
    seq = 0
    line_count = 0

    flist = []
    
    while True:
        fpath_txt = os.path.join(in_dir, '{0}.{1}'.format(BASENAME, seq)) 
        fpath_gz  = os.path.join(in_dir, '{0}.{1}.gz'.format(BASENAME, seq))
        if seq == 0: fpath_txt = os.path.join(in_dir, BASENAME)
            
        if   os.path.exists(fpath_txt): flist.append(fpath_txt)
        elif os.path.exists(fpath_gz): flist.append(fpath_gz)
        else: break

        seq += 1

    def parse(fpath):
        print('Reading', fpath, '...', end=' ')
        sys.stdout.flush()
                
        in_fo = gzip.open(fpath, 'rt') if fpath.endswith('.gz') else open(fpath)
        lc= filter_logs(in_fo, fo)
        print('got', lc, 'lines')
        return lc

    line_counts = map(parse, flist)

    return sum(line_counts)

def parse_logs(in_f, out_f, total=None):
    today = datetime.datetime.now()
    year = today.year
    dt_len = len('Jul  9 06:26:24')
    month_set = set(range(today.month, 13))
    re_ptn = re.compile('Failed password for( invalid user|)\s+(\S.+) from (\S+)')

    next_output = 0.1

    print('Parsing...')
    for idx, line in enumerate(in_f):
        if total and idx / total > next_output:
            print('... {0}%'.format(int(idx * 100 / total)))
            next_output += 0.1
            
        dt_str = line[:dt_len]
        dt = datetime.datetime.strptime('{0} {1}'.format(year, dt_str),
                                        '%Y %b %d %H:%M:%S')
        month_set.add(dt.month)
        if (len(month_set) == 12 and dt.month == 12 and dt.day == 31):
            year -= 1
            month_set = set()
            dt = dt.replace(year=year)

        mo = re_ptn.search(line[dt_len:])

        if not mo:
            print('parse error:', dt, line)
            continue
        
        obj = {
            'remote': mo.group(3),
            'invalid user': (mo.group(1) == ' invalid user'),
            'user': mo.group(2),
            'ts': time.mktime(dt.timetuple()),
            'dt': dt.strftime('%Y-%m-%dT%H:%M:%S'),
        }

        out_f.write(msgpack.packb(obj))

    print('done')
    return
        
def load_data(target_dir, out_file):
    tmp_fd, tmp_path = tempfile.mkstemp()
    tmp_fo = os.fdopen(tmp_fd, 'w')
    print('created temp file:', tmp_path)

    total_lines = dump_ssh_log(target_dir, tmp_fo)
    tmp_fo.close()

    parse_logs(open(tmp_path), open(out_file, 'wb'), total_lines)
    os.unlink(tmp_path)
    print('removed temp file:', tmp_path)
    
def main():
    if len(sys.argv) != 3:
        print('usage) {0} <in_dir> <out_file>'.format(sys.argv[0]))
        exit(1)
        
    in_dir = sys.argv[1]
    out_file = sys.argv[2]
    print('Input from: ', in_dir)
    print('Output to:  ', out_file)

    load_data(in_dir, out_file)
    
    
if __name__ == '__main__': main()    
