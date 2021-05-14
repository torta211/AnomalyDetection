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

import abc
import os
import pandas
import sys
import six
from datetime import datetime
from util import get_probation_period, create_path


@six.add_metaclass(abc.ABCMeta)
class AnomalyDetector(object):
    """
    Base class for all anomaly detectors. When inheriting from this class please
    take note of which methods MUST be overridden, as documented below.
    """
    def __init__(self, data_set, probationary_percent):
        self.data_set = data_set
        self.probationary_period = get_probation_period(
        probationary_percent, data_set.data.shape[0])

        self.input_min = self.data_set.data["value"].min()
        self.input_max = self.data_set.data["value"].max()

    def initialize(self):
        """
        Do anything to initialize your detector in before calling run.
        Pooling across cores forces a pickling operation when moving objects from
        the main core to the pool and this may not always be possible. This function
        allows you to create objects within the pool itself to avoid this issue.
        """
        pass

    def get_additional_headers(self):
        """
        Returns a list of strings. Subclasses can add in additional columns per
        record.

        This method MAY be overridden to provide the names for those
        columns.
        """
        return []

    @abc.abstractmethod
    def handle_record(self, input_data):
        """
        Returns a list [anomalyScore, *]. It is required that the first
        element of the list is the anomalyScore. The other elements may
        be anything, but should correspond to the names returned by
        getAdditionalHeaders().

        This method MUST be overridden by subclasses
        """
        raise NotImplementedError

    def get_header(self):
        """
        Gets the outputPath and all the headers needed to write the results files.
        """
        headers = ["timestamp", "value", "anomaly_score"]
        headers.extend(self.get_additional_headers())
        return headers

    def run(self):
        """
        Main function that is called to collect anomaly scores for a given file.
        """
        headers = self.get_header()

        rows = []
        for i, row in self.data_set.data.iterrows():
            input_data = row.to_dict()
            detector_values = self.handle_record(input_data)
            output_row = list(row) + list(detector_values)
            rows.append(output_row)

            # Progress report
            if (i % 1000) == 0:
                print "."
                sys.stdout.flush()

        return pandas.DataFrame(rows, columns=headers)


def detect_data_set(args):
    """
    Function called in each detector process that run the detector that it is
    given.
    """
    (i, detector_instance, detector_name, labels, output_dir, relative_path) = args

    relative_dir, file_name = os.path.split(relative_path)
    file_name = detector_name + "_" + file_name
    output_path = os.path.join(output_dir, detector_name, relative_dir, file_name)
    create_path(output_path)

    print "%s: Beginning detection with %s for %s" % (i, detector_name, relative_path)
    detector_instance.initialize()

    results = detector_instance.run()

    # label=1 for relaxed windows, 0 otherwise
    results["label"] = labels

    results.to_csv(output_path, index=False)

    print "%s: Completed processing %s records at %s" % (i, len(results.index), datetime.now())
    print "%s: Results have been written to %s" % (i, output_path)
