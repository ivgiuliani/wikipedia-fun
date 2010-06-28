#!/usr/bin/env python
"""
Count the number of pages in a xml dump
"""

import codecs
import os
import sys

def main(args):
    try:
        wikidump = args[1]
    except (KeyError, IndexError):
        sys.stderr.write("missing wikipedia dump\n")
        return False

    p = Parser(wikidump)
    p.run()
    print "There are %d articles in %s" % (p.get_count(), wikidump)

class Parser(object):
    """
    Count the number of articles in an xml dump
    """
    def __init__(self, wikipath):
        self.path = wikipath
        self.count = 0

    def run(self):
        dump = open(self.path, "r")
        for line in dump:
            if line.startswith("Title:"):
                self.count += 1
        dump.close()

    def get_count(self):
        return self.count
    
if __name__ == '__main__':
    sys.exit(main(sys.argv))
