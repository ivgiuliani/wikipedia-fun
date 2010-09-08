#!/usr/bin/env python

"""
Create a CSV for openheatmap from a wikipedia georeferenced pages' dump
(downlodable from http://toolserver.org/~kolossos/wp-world/pg-dumps/)
See http://de.wikipedia.org/wiki/Wikipedia:WikiProjekt_Georeferenzierung/Wikipedia-World/en
"""

import csv
import math
import sys

# Distance limit (in kilometers) to cluster points together
DISTANCE_LIMIT = 50

class PointNode(object):
    NW, NE, SW, SE = 0, 1, 2, 3
    def __init__(self, lat, lon):
        self.lat, self.lon = float(lat), float(lon)
        self.nodes = [None] * 4
        self.aggregate_no = 1

    def __str__(self):
        return "%s, %s" % (self.lat, self.lon)

def to_degrees(rad):
    return rad * (180.0 / math.pi)

def to_radians(deg):
    return deg * (math.pi / 180.0)

def point_distance(p1, p2):
    """
    Returns the distance between two points in kilometers using the pythagorean
    formula with parallel meridians. This formula is not very accurate but still
    is significantly faster (and simpler) than the alternatives
    """
    R = 6371 # in Km, avoid float precision in order to gain some performance
    lat1, lat2 = to_radians(p1.lat), to_radians(p2.lat)
    lon1, lon2 = to_radians(p1.lon), to_radians(p2.lon)
    d_lat, d_lon = lat1 - lat2, lon1 - lon2
    return R * math.sqrt(d_lat**2 + d_lon**2)

def point_midpoint(p1, p2):
    """
    Calculates the midpoint of two points. Returns a PointNode instance
    """
    lat1, lat2 = to_radians(p1.lat), to_radians(p2.lat)
    lon1, lon2 = to_radians(p1.lon), to_radians(p2.lon)

    # again, we don't need to be very precise so a simple
    # coordinate's mean it's enough
    new_lat = to_degrees((lat1 + lat2) / 2.0)
    new_lon = to_degrees((lon1 + lon2) / 2.0)

    return PointNode(new_lat, new_lon)

def main(args):
    try:
        path = args[1]
    except IndexError:
        sys.stderr.write("Usage: %s <path>\n" % args[0])
        return False

    tree = build_tree(path)
    print_coords(tree)

    return True

def build_tree(path):
    """
    Build a quad tree of points and cluster them together when they're close
    to each other. Returns the tree's root.
    """
    f = csv.reader(open(path, "r"))
    # skip the first line that belongs to the header
    f.next()

    root = None
    for line in f:
        lang, title, lat, lon = line[:4]
        p = PointNode(lat, lon)

        # analyze only georeferenced pages from english wikipedia to
        # keep the number of points low. A more accurate analysis
        # should include all the points
        # if lang != "en": continue

        root = qtree_insert(root, p)

    return root

def qtree_insert(root, node):
    """
    Insert a point into the quad tree substituting a node with its
    midpoint if the nodes are near to each other (less than DISTANCE_LIMIT)
    """
    if not root:
        return node

    # if we are under the distance limit, replace the root node with the
    # midpoint of the two nodes
    if point_distance(root, node) < DISTANCE_LIMIT:
        c = point_midpoint(root, node)
        c.nodes = root.nodes
        c.aggregate_no = root.aggregate_no + 1
        root = c
    else:
        # otherwise just insert the node where it belongs

        # exploit PointNode child indexing (with NW being 0 we just need to add
        # the proper number to get what we need)
        pos = PointNode.NW
        if node.lat > root.lat:
            pos += 2
        if node.lon > root.lon:
            pos += 1

        root.nodes[pos] = qtree_insert(root.nodes[pos], node)
    return root

def print_coords(root):
    "Print the coordinates from the quad tree through a DFS visit"
    out = csv.writer(sys.stdout)
    out.writerow(["lat", "lon", "value"])
    def dfs(root, func):
        if not root: return
        func(root.lat, root.lon, root.aggregate_no)
        for node in root.nodes:
            dfs(node, func)
    dfs(root, lambda lat, lon, value: out.writerow([lat, lon, value]))

if __name__ == '__main__':
    sys.exit(main(sys.argv))
