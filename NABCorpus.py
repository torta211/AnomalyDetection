# ----------------------------------------------------------------------
# Copyright (C) 2014-2015, Numenta, Inc.  Unless you have an agreement
# with Numenta, Inc., for a separate license for this software code, the
# following terms and conditions apply:
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Affero Public License for more details.
#
# You should have received a copy of the GNU Affero Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
# http://numenta.org/licenses/
# ----------------------------------------------------------------------

"""
This contains the objects to store and manipulate a database of csv files.
"""

import copy
import os
import json
import pandas
import itertools

from util import absolute_file_paths, create_path, deepmap, strp


class DataFile(object):
    """
    Class for storing and manipulating a single datafile.
    Data is stored in pandas.DataFrame
    """

    def __init__(self, srcPath):
        """
        @param srcPath (string)   Filename of datafile to read.
        """
        self.srcPath = srcPath

        self.fileName = os.path.split(srcPath)[1]

        self.data = pandas.read_csv(self.srcPath,
                                    header=0, parse_dates=[0])

    def write(self, newPath=None):
        """Write datafile to self.srcPath or newPath if given.

        @param newPath (string)   Path to write datafile to. If path is not given,
                                  write to source path
        """

        path = newPath if newPath else self.srcPath
        self.data.to_csv(path, index=False)

    def modifyData(self, columnName, data=None, write=False):
        """Add columnName to datafile if data is given otherwise remove
        columnName.

        @param columnName (string)          Name of the column in the datafile to
                                            either add or remove.

        @param data       (pandas.Series)   Column data to be added to datafile.
                                            Data length should be as long as the
                                            length of other columns.

        @param write      (boolean) Flag to choose whether to write modifications to
                                    source path.
        """
        if isinstance(data, pandas.Series):
            self.data[columnName] = data
        else:
            if columnName in self.data:
                del self.data[columnName]

        if write:
            self.write()

    def getTimestampRange(self, t1, t2):
        """Given timestamp range, get all records that are within that range.

        @param t1   (int)   Starting timestamp.

        @param t2   (int)   Ending timestamp.

        @return     (list)  Timestamp and value for each time stamp within the
                            timestamp range.
        """
        tmp = self.data[self.data["timestamp"] >= t1]
        ans = tmp[tmp["timestamp"] <= t2]["timestamp"].tolist()
        return ans

    def __str__(self):
        ans = ""
        ans += "path:                %s\n" % self.srcPath
        ans += "file name:           %s\n" % self.fileName
        ans += "data size:         ", self.data.shape()
        ans += "sample line: %s\n" % ", ".join(self.data[0])
        return ans


class Corpus(object):
    """
    Class for storing and manipulating a corpus of data where each datafile is
    stored as a DataFile object.
    """

    def __init__(self, srcRoot):
        """
        @param srcRoot    (string)    Source directory of corpus.
        """
        self.srcRoot = srcRoot
        self.dataFiles = self.getDataFiles()
        self.numDataFiles = len(self.dataFiles)

    def getDataFiles(self):
        """
        Collect all CSV data files from self.srcRoot directory.

        @return (dict)    Keys are relative paths (from self.srcRoot) and values are
                          the corresponding data files.
        """
        filePaths = absolute_file_paths(self.srcRoot)
        dataSets = [DataFile(path) for path in filePaths if ".csv" in path]

        def getRelativePath(srcRoot, srcPath):
            return srcPath[srcPath.index(srcRoot) + len(srcRoot):] \
                .strip(os.path.sep).replace(os.path.sep, "/")

        return {getRelativePath(self.srcRoot, d.srcPath): d for d in dataSets}

    def addColumn(self, columnName, data, write=False):
        """
        Add column to entire corpus given columnName and dictionary of data for each
        file in the corpus. If newRoot is given then corpus is copied and then
        modified.

        @param columnName   (string)  Name of the column in the datafile to add.

        @param data         (dict)    Dictionary containing key value pairs of a
                                      relative path and its corresponding
                                      datafile (as a pandas.Series).

        @param write        (boolean) Flag to decide whether to write corpus
                                      modificiations or not.
        """

        for relativePath in list(self.dataFiles.keys()):
            self.dataFiles[relativePath].modifyData(
                columnName, data[relativePath], write=write)

    def removeColumn(self, columnName, write=False):
        """
        Remove column from entire corpus given columnName. If newRoot if given then
        corpus is copied and then modified.

        @param columnName   (string)  Name of the column in the datafile to add.

        @param write        (boolean) Flag to decide whether to write corpus
                                      modificiations or not.
        """
        for relativePath in list(self.dataFiles.keys()):
            self.dataFiles[relativePath].modifyData(columnName, write=write)

    def copy(self, newRoot=None):
        """Copy corpus to a newRoot which cannot already exist.

        @param newRoot      (string)      Location of new directory to copy corpus
                                          to.
        """
        if newRoot[-1] != os.path.sep:
            newRoot += os.path.sep
        if os.path.isdir(newRoot):
            print("directory already exists")
            return None
        else:
            create_path(newRoot)

        newCorpus = Corpus(newRoot)
        for relativePath in list(self.dataFiles.keys()):
            newCorpus.addDataSet(relativePath, self.dataFiles[relativePath])
        return newCorpus

    def addDataSet(self, relativePath, dataSet):
        """Add datafile to corpus given its realtivePath within the corpus.

        @param relativePath     (string)      Path of the new datafile relative to
                                              the corpus directory.

        @param datafile          (datafile)     Data set to be added to corpus.
        """
        self.dataFiles[relativePath] = copy.deepcopy(dataSet)
        newPath = self.srcRoot + relativePath
        create_path(newPath)
        self.dataFiles[relativePath].srcPath = newPath
        self.dataFiles[relativePath].write()
        self.numDataFiles = len(self.dataFiles)

    def getDataSubset(self, query):
        """
        Get subset of the corpus given a query to match the datafile filename or
        relative path.

        @param query        (string)      Search query for obtainin the subset of
                                          the corpus.

        @return             (dict)        Dictionary containing key value pairs of a
                                          relative path and its corresponding
                                          datafile.
        """
        ans = {}
        for relativePath in list(self.dataFiles.keys()):
            if query in relativePath:
                ans[relativePath] = self.dataFiles[relativePath]
        return ans


class CorpusLabel(object):
    """
    Class to store and manipulate a single set of labels for the whole
    benchmark corpus.
    """

    def __init__(self, path, corpus):
        """
        Initializes a CorpusLabel object by getting the anomaly windows and labels.
        When this is done for combining raw user labels, we skip getLabels()
        because labels are not yet created.

        @param path    (string)      Name of file containing the set of labels.
        @param corpus  (nab.Corpus)  Corpus object.
        """
        self.path = path

        self.windows = None
        self.labels = None

        self.corpus = corpus
        self.getWindows()

        if "raw" not in self.path:
            # Do not get labels from files in the path nab/labels/raw
            self.getLabels()

    def getWindows(self):
        """
        Read JSON label file. Get timestamps as dictionaries with key:value pairs of
        a relative path and its corresponding list of windows.
        """

        def found(t, data):
            f = data["timestamp"][data["timestamp"] == pandas.Timestamp(t)]
            exists = (len(f) == 1)

            return exists

        with open(os.path.join(self.path)) as windowFile:
            windows = json.load(windowFile)

        self.windows = {}

        for relativePath in list(windows.keys()):

            self.windows[relativePath] = deepmap(strp, windows[relativePath])

            if len(self.windows[relativePath]) == 0:
                continue

            data = self.corpus.dataFiles[relativePath].data
            if "raw" in self.path:
                timestamps = windows[relativePath]
            else:
                timestamps = list(itertools.chain.from_iterable(windows[relativePath]))

            # Check that timestamps are present in dataset
            if not all([found(t, data) for t in timestamps]):
                raise ValueError("In the label file %s, one of the timestamps used for "
                                 "the datafile %s doesn't match; it does not exist in "
                                 "the file. Timestamps in json label files have to "
                                 "exactly match timestamps in corresponding datafiles."
                                 % (self.path, relativePath))

    def validateLabels(self):
        """
        This is run at the end of the label combining process (see
        scripts/combine_labels.py) to validate the resulting ground truth windows,
        specifically that they are distinct (unique, non-overlapping).
        """
        with open(os.path.join(self.path)) as windowFile:
            windows = json.load(windowFile)

        self.windows = {}

        for relativePath in list(windows.keys()):

            self.windows[relativePath] = deepmap(strp, windows[relativePath])

            if len(self.windows[relativePath]) == 0:
                continue

            num_windows = len(self.windows[relativePath])
            if num_windows > 1:
                if not all([(self.windows[relativePath][i + 1][0]
                             - self.windows[relativePath][i][1]).total_seconds() >= 0
                            for i in range(num_windows - 1)]):
                    raise ValueError("In the label file %s, windows overlap." % self.path)

    def getLabels(self):
        """
        Get Labels as a dictionary of key-value pairs of a relative path and its
        corresponding binary vector of anomaly labels. Labels are simply a more
        verbose version of the windows.
        """
        self.labels = {}

        for relativePath, dataSet in self.corpus.dataFiles.items():
            if relativePath in self.windows:
                windows = self.windows[relativePath]

                labels = pandas.DataFrame({"timestamp": dataSet.data["timestamp"]})
                labels['label'] = 0

                for t1, t2 in windows:
                    moreThanT1 = labels[labels["timestamp"] >= t1]
                    betweenT1AndT2 = moreThanT1[moreThanT1["timestamp"] <= t2]
                    indices = betweenT1AndT2.loc[:, "label"].index
                    labels["label"].values[indices.values] = 1

                self.labels[relativePath] = labels

            else:
                print("Warning: no label for datafile", relativePath)
