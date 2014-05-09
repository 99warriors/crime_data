import pdb
import crime_data.constants
import python_utils.python_utils.caching as caching
import python_utils.python_utils.utils as utils
import pandas as pd
import numpy as np
import functools
import itertools
import math
import matplotlib.pyplot as plt

@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_burglary_raw_data():
    incident = pd.read_csv(crime_data.constants.incidents_raw_file, index_col = 0)
    burglary = pd.read_csv(crime_data.constants.burglary_raw_file, index_col = 0)
    incident_burglary = pd.concat([incident, burglary], axis=1, join='inner')
    years = incident_burglary['date_from'].apply(utils.date_string_to_year)
    incident_burglary['year'] = years
    return incident_burglary


@caching.read_fxn_decorator(lambda ans, path: pd.DataFrame.to_csv(ans, path), lambda identifier, *args, **kwargs: crime_data.constants.merged_pattern_raw_file, None)
def get_patterns():
    """
    coerces pincnum index to int
    """
    def fix_pincnum(s):
        s = str(s)
        try:
            import re
            if pd.isnull(s):
                return s
            s = re.sub('\-', '0', s)
            s = str(int(float(s)))
            if len(s) == 9:
                return int('%s0%s' % (s[0:4], s[5:]))
            else:
                return int(s)
        except ValueError:
            return np.nan

    files = ['%s/%dPatterns.csv' % (crime_data.constants.pattern_folder, year) for year in range(1997,2007)]
    dfs = [pd.read_csv(file, index_col = None) for file in files]
    df = pd.concat(dfs, ignore_index=True)
    df['pincnum'] = df['File #'].apply(fix_pincnum)
    df = df.rename(columns = {'Pattern #':'pattern'})
    df = df[[np.isnan(x) == False for x in df.pincnum]]
    df = df.drop_duplicates(['pincnum'])
    df.index = [int(x) for x in df['pincnum']]
    return df


@caching.default_cache_fxn_decorator
@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_processed_burglary_data():
    """
    burglary data with latlngs, pattern info.  index(pincnum) is an int
    """
    incident_burglary = get_burglary_raw_data()
    patterns = get_patterns()
    assert False
    import python_utils.exceptions
 
    incident_burglary['latlng'] = incident_burglary['address'].apply(utils.exception_catcher_fxn_decorator(np.nan, (python_utils.exceptions.TooLazyToComputeException,))(lambda s: (utils.get_lat_lng('%s cambridge, MA' % s)['lat'], utils.get_lat_lng('%s cambridge, MA' % s)['lng'])))

    incident_burglary['lat'] = incident_burglary['latlng'].apply(utils.exception_catcher_fxn_decorator(np.nan, (TypeError,))(lambda x: x[0]))
    incident_burglary['lng'] = incident_burglary['latlng'].apply(utils.exception_catcher_fxn_decorator(np.nan, (TypeError,))(lambda x: x[1]))

    import string
    from datetime import datetime
    incident_burglary['date_object'] = incident_burglary['date_from'].apply(lambda s:datetime(int(string.split(string.split(s)[0], sep='/')[2]),int(string.split(string.split(s)[0], sep='/')[0]),int(string.split(string.split(s)[0], sep='/')[1])))
    min_date = min(incident_burglary['date_object'])
    seconds_in_year = 31622400.0
    incident_burglary['date_num'] = incident_burglary['date_object'].apply(lambda a_date: (a_date - min_date).total_seconds() / seconds_in_year)

    no_latlng_nas = incident_burglary[pd.isnull(incident_burglary['latlng'])==False]

    merged = pd.merge(patterns[['pattern']], no_latlng_nas, how='right', right_index=True, left_index = True)
    merged['in_pattern'] = (pd.isnull(merged['pattern']) == False)
    return merged


def get_all_burglary_patterns():
    d = get_processed_burglary_data()
    return d['pattern'].unique()


class house_break_f(utils.f):

    def __repr__(self):
        return self.col_name

    def __init__(self, col_name):
        self.d, self.col_name = get_processed_burglary_data(), col_name

    def __call__(self, data_id):
        try:
            return self.d[self.col_name][data_id]
        except:
            pdb.set_trace()


class in_pattern_f(utils.f):

    def __repr__(self):
        return 'in_pattern_f'

    def __init__(self):
        self.f = house_break_f('pattern')

    def __call__(self, data_id):
        return not pd.isnull(self.f(data_id))


class pattern_id_f(utils.f):

    def __equals__(self, other):
        return repr(self) == str(other)

    def __repr__(self):
        return 'pattern'

    def __init__(self):
        self.d = get_processed_burglary_data()

    def __call__(self, data_id):
        return int(self.d['pattern'][data_id])


class AllHouseBurglaryIterableWithPattern(utils.DataIdIterable):
    """
    iterable over housebreak crimes that belong to patterns
    """
    def __init__(self):
        d = get_processed_burglary_data()
        self.ids = d[pd.isnull(d.pattern)==False].index.to_series()

    def __iter__(self):
        return iter(self.ids)


class AllHouseBurglaryIterable(utils.DataIdIterable):
    """
    iterable over housebreak crimes that belong to patterns
    """
    def __init__(self, max_num=None):
        self.max_num = max_num
        d = get_processed_burglary_data()
        self.ids = d.index.to_series()

    def __iter__(self):
        if self.max_num == None:
            return iter(self.ids)
        else:
            return iter(self.ids[0:self.max_num])


def plot_pattern_helper(data):
    fig, ax = plt.subplots()
    data_xys = [utils.latlng_to_xy(*datum.location) for datum in data]
    ax.scatter([xy[0] for xy in data_xys], [xy[1] for xy in data_xys], color = 'black', s = 1)
    return fig, ax


def plot_pattern_timeline(data):
    """
    finds the labelled patterns in data
    """
    id_to_years = {}
    for datum in data:
        if datum.in_pattern:
            try:
                id_to_years[datum.which_pattern].append(datum.time)
            except KeyError:
                id_to_years[datum.which_pattern] = [datum.time]
                
    for key, val in id_to_years.iteritems():
        id_to_years[key] = sorted(val)

    pattern_list = sorted([(key, val) for (key, val) in id_to_years.iteritems()], key = lambda x:x[1][0])

    fig, ax = plt.subplots()

    size = 4
    marker = '|'

    for i, (pattern_id, years) in enumerate(pattern_list):
        ax.scatter(years, np.ones(len(years)) * i, color = 'black', s = size, marker = marker)

    ax.set_xlabel('years from beg of dataset')
    ax.set_xlim((0, None))
    ax.set_ylim((0, None))
    fig.suptitle('pattern timeline')
    ax.set_ylabel('pattern index')

    return fig
