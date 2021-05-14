import pandas as pd
import numpy as np
import datetime


directory = "C:/EreBere/timeseries/data/Yahoo"
output_directory = "C:/EreBere/timeseries/data/YahooWithTime"
base = datetime.datetime(2021, 1, 1)
for i in range(1, 68):
    fname = "real_" + str(i) + ".csv"
    raw = pd.read_csv(directory + "/" + fname)
    arr = np.array([base + datetime.timedelta(seconds=i) for i in xrange(raw.shape[0])])
    raw['timestamp'] = arr
    raw[['timestamp', 'value']].to_csv(output_directory + "/" + fname, index=False)
