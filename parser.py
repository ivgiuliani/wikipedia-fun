"""
Parse a wikipedia dump extracting georeferenced pages
"""

import re
import sys
from xml.parsers.expat import *

ITEM_SEP = "--------------------"

class Parser(object):
    """
    Parse the whole wiki file and extracts only georeferenced entries
    """
    STATE_NONE, STATE_TITLE, STATE_TEXT = 0, 1, 2
    STATUSES = {
        "title": STATE_TITLE,
        "text":  STATE_TEXT,
    }
    HASCOORD_REGEXP = re.compile(r"{{coord"                   \
                                    "([A-z0-9_:\|=\.\(\)]*)"  \
                                    "\|display=title"         \
                                    "([A-z0-9_:\|=\.\(\)]*)"  \
                                  "}}")

    def __init__(self, wikipath, dumppath):
        """
        Init the class. wikipath is the full wikipedia dump while
        dumppath is the file where the georeferenced pages should
        be saved (this file will be overridden if it already exists)
        """
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

        self.extracted_dump = dumppath
        self.extracted_count = 0

    def run(self):
        dump = open(self.dumpfile, "r")
        self.extracted = open(self.extracted_dump, "w")
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

            coord = self.get_georeference(self.current["text"])
            if coord:
                self.add_item(coord, self.current)

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

    def get_georeference(self, data):
        """
        Check whether a wiki entry is georeferenced and if it is
        return the extracted georeference in wikipedia format
        """
        if data:
            geo = self.HASCOORD_REGEXP.search(data)
            if geo:
                return geo.group()
        return None

    def add_item(self, coord, item):
        "Extract the current item to the georeferenced dump"
        self.extracted_count += 1
        self.extracted.write("Title: %s\n" % item["title"])
        self.extracted.write("Coord: %s\n" % coord)
        self.extracted.write("%s\n\n%s\n\n" % (item["text"], ITEM_SEP))

        sys.stdout.write("[%8d] Extracted %s\n" % (self.extracted_count, item["title"]))


class DumpParser(object):
    "Parse the georeferenced dump"
    pass
