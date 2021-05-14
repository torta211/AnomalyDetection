from NumentaDetectorTM import NumentaDetectorTM
from CADOSEDetector import ContextOSEDetector
from KnnCadDetector import KnncadDetector
from skyline.EarthGeckoSkylineDetector import EarthgeckoSkylineDetector
from AnomalyDetector import detect_data_set
from NABCorpus import Corpus, CorpusLabel
import time


done = 0

def run_on_file(filename, detectorConstructor, detectorName):
    global done, results_dir, corpus, corpus_label
    done += 1
    args = (i, detectorConstructor(data_set=corpus.dataFiles[filename], probationary_percent=0.15),
            detectorName, corpus_label.labels[filename]["label"], results_dir, filename)
    detect_data_set(args)


if __name__ == "__main__":
    results_dir = "C:\\EreBere\\timeseries\\offi"

    #data_dir = "C:\\EreBere\\timeseries\\data\\YahooWithTime"
    #data_dir = "C:\\EreBere\\timeseries\\AnomalyDetection\\data\\streams"
    data_dir = "C:\\EreBere\\timeseries\\idosorok"
    label_path = "C:\\EreBere\\timeseries\\AnomalyDetection\\data\\labels\\combined_windows.json"

    corpus = Corpus(data_dir)
    corpus_label = CorpusLabel(label_path, corpus)
    i = 0
    run_on_file("sin.csv", NumentaDetectorTM, "NumentaDetectorTM")
    # times_cadose = []
    # times_knn = []
    # times_htm = []
    # times_skyline = []
    # for i in range(1, 10):
    #     t1 = time.time()
    #     run_on_file("real_" + str(i) + ".csv", ContextOSEDetector, "ContextOSEDetector")
    #     t2 = time.time()
    #     run_on_file("real_" + str(i) + ".csv", KnncadDetector, "KnncadDetector")
    #     t3 = time.time()
    #     run_on_file("real_" + str(i) + ".csv", NumentaDetectorTM, "NumentaDetectorTM")
    #     t4 = time.time()
    #     run_on_file("real_" + str(i) + ".csv", EarthgeckoSkylineDetector, "EarthgeckoSkylineDetector")
    #     t5 = time.time()
    #     times_cadose.append(t2 - t1)
    #     times_knn.append(t3 - t2)
    #     times_htm.append(t4 - t3)
    #     times_skyline.append(t5 - t4)
    #     print(t2 - t1, t3 - t2, t4 - t3, t5 - t4)
    # # run_on_file("Twitter_volume_AMZN.csv")
    # print("cadose", sum(times_cadose) / len(times_cadose))
    # print("knn", sum(times_knn) / len(times_knn))
    # print("htm", sum(times_htm) / len(times_htm))
    # print("skyline", sum(times_skyline) / len(times_skyline))

    t1 = time.time()
    run_on_file("A.csv", ContextOSEDetector, "ContextOSEDetector")
    # run_on_file("B.csv", ContextOSEDetector, "ContextOSEDetector")
    # run_on_file("C.csv", ContextOSEDetector, "ContextOSEDetector")
    #
    t2 = time.time()
    run_on_file("A.csv", KnncadDetector, "KnncadDetector")
    # run_on_file("B.csv", KnncadDetector, "KnncadDetector")
    # run_on_file("C.csv", KnncadDetector, "KnncadDetector")
    #
    t3 = time.time()
    run_on_file("A.csv", NumentaDetectorTM, "NumentaDetectorTM")
    # run_on_file("B.csv", NumentaDetectorTM, "NumentaDetectorTM")
    # run_on_file("C.csv", NumentaDetectorTM, "NumentaDetectorTM")
    #
    t4 = time.time()
    run_on_file("A.csv", EarthgeckoSkylineDetector, "EarthgeckoSkylineDetector")
    # run_on_file("B.csv", EarthgeckoSkylineDetector, "EarthgeckoSkylineDetector")
    # run_on_file("C.csv", EarthgeckoSkylineDetector, "EarthgeckoSkylineDetector")
    t5 = time.time()
    print(t2 - t1, t3 - t2, t4 - t3, t5 - t4)
