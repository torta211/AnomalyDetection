import pandas as pd


class HSTreeDetector:
    def __init__(self):
        self.lag_rolling = 100
        self.tree_list = []


    def build_trees(self, num_trees, depth, dimension_mins, dimension_maxs):
        num_dimensions = len(dimension_maxs)
        

