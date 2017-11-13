#!/usr/bin/env python

import argparse
import os
import struct


def one_file_to_many(inPath):
    files = []
    with open(inPath, 'rb') as ifs:
        while True:
            chunk = ifs.read(4)
            if chunk == '':
                break
            fileNameLen = struct.unpack('I', chunk)[0]
            fileName = ifs.read(fileNameLen)
            chunk = ifs.read(4)
            if chunk == '':
                raise Exception('Expected 4 bytes')
            dataLen = struct.unpack('I', chunk)[0]
            files.append((fileName, ifs.read(dataLen)))
    return files


def main(infile, outdir):
    print 'Extracting files from %s' % infile
    os.makedirs(outdir)
    count = 0
    for name, data in one_file_to_many(infile):
        outPath = os.path.join(outdir, name)
        with open(outPath, 'wb') as ofs:
            ofs.write(data)
        count += 1
    print 'Done! [Extracted %d files into %s]' % (count, outdir) 


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', type=str)
    parser.add_argument('outdir', type=str)
    args = parser.parse_args()
    main(args.infile, args.outdir)
