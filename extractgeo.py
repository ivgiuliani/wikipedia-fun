#!/usr/bin/env python
"""
Extracts georeferenced pages from the xml dump
"""

import os
import sys

from parser import Parser

def main(args):
    try:
        wikidump = args[1]
    except (KeyError, IndexError):
        sys.stderr.write("missing wikipedia dump\n")
        return False

    parse_dump(wikidump)

def parse_dump(wikidump):
    wikipath = os.path.normpath(os.path.realpath(wikidump))
    dumppath = wikipath + ".extracted"

    if not os.path.exists(wikipath):
        sys.stderr.write("%s does not exists\n" % wikipath)

    sys.stdout.write("Parsing %s...\n" % wikipath)

    parser = Parser(wikipath, dumppath)
    parser.run()
    sys.stdout.write("Job completed\n")

if __name__ == '__main__':
    sys.exit(main(sys.argv))
