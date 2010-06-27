#!/usr/bin/env python
"""
Extracts georeferenced pages from the xml dump
"""

import os
import re
import sys
import time

from xml.parsers.expat import *
from multiprocessing import Process, Manager
from Queue import Empty as QueueEmpty

ITEM_SEP = "--------------------"

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

    parser = Parser(wikipath)
    extractor = Extractor(dumppath)

    manager = Manager()
    queue = manager.Queue()
    parser_running = manager.Event()

    parser_running.set()
    parser_process = Process(target=parser.run, args=(queue, ), name="Parser")
    parser_process.start()

    extractor_process = Process(target=extractor.run, args=(queue, parser_running), name="Extractor")
    extractor_process.start()

    parser_process.join()
    sys.stdout.write("QUIT: parser\n")
    parser_running.clear()

    extractor_process.join()
    sys.stdout.write("QUIT: extractor\n")

    sys.stdout.write("Job completed\n")


class Parser(object):
    """
    Parse the whole wiki file and extracts only georeferenced entries
    """
    STATE_NONE, STATE_TITLE, STATE_TEXT = 0, 1, 2
    STATUSES = {
        "title": STATE_TITLE,
        "text":  STATE_TEXT,
    }

    def __init__(self, wikipath):
        self.dumpfile = wikipath
        self.status = Parser.STATE_NONE
        self.current = {
            "title": None,
            "text": None,
        }
        self.current_tag = None

        # init parser
        self.parser = ParserCreate()
        self.parser.StartElementHandler = self.startElement
        self.parser.EndElementHandler = self.endElement
        self.parser.CharacterDataHandler = self.textReceived

    def run(self, queue):
        self.queue = queue
        dump = open(self.dumpfile, "r")
        self.parser.ParseFile(dump)
        dump.close()

    def startElement(self, name, attrs):
        self.current_tag = name.lower()
        try:
            self.status = Parser.STATUSES[name]
        except KeyError:
            self.status = Parser.STATE_NONE
    
    def endElement(self, name):
        if name.lower() == "page":
            self.current["title"] = self.current["title"].strip()
            self.queue.put(self.current.copy())

            self.current["title"] = None
            self.current["text"] = None
        self.status = Parser.STATE_NONE

    def textReceived(self, data):
        if not data: return
        data = data.encode("utf-8")

        if self.status in Parser.STATUSES.values():
            if not self.current[self.current_tag]:
                self.current[self.current_tag] = ""
            self.current[self.current_tag] += data


class Extractor(object):
    """
    Saves the georeferenced wiki pages extracted from the parser
    and dump them on the specified file
    """
    HASCOORD_REGEXP = re.compile(r"{{coord"                   \
                                    "([A-z0-9_:\|=\.\(\)]*)"  \
                                    "\|display=title"         \
                                    "([A-z0-9_:\|=\.\(\)]*)"  \
                                  "}}")

    def __init__(self, dumpfile):
        self.dumpfile = dumpfile

    def run(self, queue, parser_running):
        self.queue = queue
        self.parser_running = parser_running
        self.dump = open(self.dumpfile, "w")
        self.extract()
        self.dump.flush()
        self.dump.close()

    def extract(self):
        self.counter = 0
        while self.parser_running.is_set() or not self.queue.empty():
            try:
                item = self.queue.get(timeout=1)
            except QueueEmpty:
                sys.stdout.write("[ Empty queue ]\n")
                time.sleep(1)
                continue
            coord = self.get_georeference(item["text"])
            if coord:
                self.counter += 1
                self.dump.write("Title: %s\n" % item["title"])
                self.dump.write("Coord: %s\n" % coord)
                self.dump.write("%s\n\n%s\n\n" % (item["text"], ITEM_SEP))

                sys.stdout.write("[%8d] Extracted %s\n" % (
                                        self.counter, item["title"]))
        sys.stdout.write("\n\nDone extracting %d items\n\n" % self.counter)

    def get_georeference(self, data):
        "Check whether a wiki entry is georeferenced"
        if data:
            geo = self.HASCOORD_REGEXP.search(data)
            if geo:
                return geo.group()
        return None

if __name__ == '__main__':
    sys.exit(main(sys.argv))
