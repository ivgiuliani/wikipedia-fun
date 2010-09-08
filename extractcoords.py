#!/usr/bin/env python

import sys

from parser import DumpParser

def main(args):
    try:
        path = args[1]
    except IndexError:
        sys.stderr.write("Usage: %s <path>\n" % args[0])
        return False

    extract_coords(path)

def extract_coords(path):
    parser = DumpParser(path)
    for item in parser:
        sys.stdout.write(item.coords + "\n")

if __name__ == '__main__':
    sys.exit(main(sys.argv))
