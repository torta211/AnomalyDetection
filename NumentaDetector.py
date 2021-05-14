# ----------------------------------------------------------------------
# Copyright (C) 2014, Numenta, Inc.  Unless you have an agreement
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

import math
from nupic.algorithms import anomaly_likelihood
from nupic.frameworks.opf.common_models.cluster_params import getScalarMetricWithTimeOfDayAnomalyParams
from nupic.frameworks.opf.model_factory import ModelFactory
from AnomalyDetector import AnomalyDetector

# Fraction outside of the range of values seen so far that will be considered
# a spatial anomaly regardless of the anomaly likelihood calculation. This
# accounts for the human labelling bias for spatial values larger than what
# has been seen so far.
SPATIAL_TOLERANCE = 0.05


class NumentaDetector(AnomalyDetector):
    """
    This detector uses an HTM based anomaly detection technique.
    """
    def __init__(self, *args, **kwargs):
        super(NumentaDetector, self).__init__(*args, **kwargs)

        self.model = None
        self.sensor_params = None
        self.anomaly_likelihood = None
        # Keep track of value range for spatial anomaly detection
        self.min_val = None
        self.max_val = None

        # Set this to False if you want to get results based on raw scores
        # without using AnomalyLikelihood. This will give worse results, but
        # useful for checking the efficacy of AnomalyLikelihood. You will need
        # to re-optimize the thresholds when running with this setting.
        self.use_likelihood = True

    def get_additional_headers(self):
        """
        Returns a list of strings.
        """
        return ["raw_score"]

    def handle_record(self, input_data):
        """
        Returns a tuple (anomaly_score, raw_score).

        Internally to NuPIC "anomaly_score" corresponds to "likelihood_score"
        and "raw_score" corresponds to "anomaly_score". Sorry about that.
        """
        # Send it to Numenta detector and get back the results
        result = self.model.run(input_data)

        # Get the value
        value = input_data["value"]

        # Retrieve the anomaly score and write it to a file
        raw_score = result.inferences["anomalyScore"]

        # Update min/max values and check if there is a spatial anomaly
        spatial_anomaly = False
        if self.min_val != self.max_val:
            tolerance = (self.max_val - self.min_val) * SPATIAL_TOLERANCE
            max_expected = self.max_val + tolerance
            min_expected = self.min_val - tolerance
            if value > max_expected or value < min_expected:
                spatial_anomaly = True
        if self.max_val is None or value > self.max_val:
            self.max_val = value
        if self.min_val is None or value < self.min_val:
            self.min_val = value

        if self.use_likelihood:
            # Compute log(anomaly likelihood)
            anomaly_score = self.anomaly_likelihood.anomalyProbability(
                input_data["value"], raw_score, input_data["timestamp"])
            log_score = self.anomaly_likelihood.computeLogLikelihood(anomaly_score)
            final_score = log_score
        else:
            final_score = raw_score

        if spatial_anomaly:
            final_score = 1.0

        return final_score, raw_score

    def initialize(self):
        # Get config params, setting the RDSE resolution
        range_padding = abs(self.input_max - self.input_min) * 0.2
        model_params = getScalarMetricWithTimeOfDayAnomalyParams(
            metricData=[0],
            minVal=self.input_min - range_padding,
            maxVal=self.input_max + range_padding,
            minResolution=0.001,
            tmImplementation="cpp"
        )["modelConfig"]

        self._setup_encoder_params(
          model_params["modelParams"]["sensorParams"]["encoders"])

        self.model = ModelFactory.create(model_params)

        self.model.enableInference({"predictedField": "value"})

        if self.use_likelihood:
            # Initialize the anomaly likelihood object
            numenta_learning_period = int(math.floor(self.probationary_period / 2.0))
            self.anomaly_likelihood = anomaly_likelihood.AnomalyLikelihood(
                learningPeriod=numenta_learning_period,
                estimationSamples=self.probationary_period - numenta_learning_period,
                reestimationPeriod=100
            )

    def _setup_encoder_params(self, encoder_params):
        # The encoder must expect the NAB-specific datafile headers
        encoder_params["timestamp_dayOfWeek"] = encoder_params.pop("c0_dayOfWeek")
        encoder_params["timestamp_timeOfDay"] = encoder_params.pop("c0_timeOfDay")
        encoder_params["timestamp_timeOfDay"]["fieldname"] = "timestamp"
        encoder_params["timestamp_timeOfDay"]["name"] = "timestamp"
        encoder_params["timestamp_weekend"] = encoder_params.pop("c0_weekend")
        encoder_params["value"] = encoder_params.pop("c1")
        encoder_params["value"]["fieldname"] = "value"
        encoder_params["value"]["name"] = "value"

        self.sensorParams = encoder_params["value"]

