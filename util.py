import os
import math
import dateutil


def make_dirs_exist(dir_name):
    """
    Makes sure a given directory exists. If not, it creates it.
    @param dir_name  (string)  Absolute directory name.
    """
    if not os.path.exists(dir_name):
        # This is being run in parallel so watch out for race condition.
        try:
            os.makedirs(dir_name)
        except OSError:
            pass


def create_path(path):
    """
    Makes sure a given path exists. If not, it creates it.
    @param path   (string) Absolute path name.
    """
    dir_name = os.path.dirname(path)
    make_dirs_exist(dir_name)


def get_probation_period(probation_percent, file_length):
    """
    Return the probationary period index.
    """
    return min(
        math.floor(probation_percent * file_length),
        probation_percent * 5000)


def absolute_file_paths(directory):
    """
    Given directory, gets the absolute path of all files within.

    @param  directory   (string)    Directory name.

    @return             (iterable)  All absolute filepaths within directory.
    """
    for dirpath, _, filenames in os.walk(directory):
        filenames = [f for f in filenames if not f[0] == "."]
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def deepmap(f, datum):
    """Deeply applies f across the datum.

    @param f      (function)    Function to map with.

    @param datum  (datum)       Object to map over.
    """
    if type(datum) == list:
        return [deepmap(f, x) for x in datum]
    else:
        return f(datum)


def strp(t):
    """
    @param t (datetime.datetime)  String of datetime with format:
                                "YYYY-MM-DD HH:mm:SS.ss".

    @return   (string)            Datetime object.
    """
    return dateutil.parser.parse(t)