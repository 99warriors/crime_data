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

@caching.default_cache_fxn_decorator
@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_burglary_raw_data():
    incident = pd.read_csv(crime_data.constants.incidents_raw_file, index_col = 0)
    burglary = pd.read_csv(crime_data.constants.burglary_raw_file, index_col = 0)
    incident_burglary = pd.concat([incident, burglary], axis=1, join='inner')
    years = incident_burglary['date_from'].apply(utils.date_string_to_year)
    incident_burglary['year'] = years
    return incident_burglary


@caching.default_cache_fxn_decorator
@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_persons_raw_data():
    return pd.read_csv(crime_data.constants.persons_raw_file, index_col = 0)


@caching.default_cache_fxn_decorator
@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_property_raw_data():
    return pd.read_csv(crime_data.constants.property_raw_file, index_col = 0)


@caching.default_cache_fxn_decorator
@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_persons_pinc_to_df():
    return {pinc:df for (pinc, df) in get_persons_raw_data().groupby(lambda x:x)}


@caching.default_cache_fxn_decorator
@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_property_pinc_to_df():
    return {pinc:df for (pinc, df) in get_property_raw_data().groupby(lambda x:x)}

pdb.set_trace()
@caching.default_cache_fxn_decorator
#@caching.default_read_fxn_decorator
@caching.default_write_fxn_decorator
def get_narratives_raw_data():
    return pd.read_csv(crime_data.constants.narratives_raw_file, index_col = 0)


def pinc_to_narrative(pinc):
    return get_narratives_raw_data().loc[pinc]


def pinc_to_verbose_data(pinc):
    ans = [None for i in range(3)]
    burglary_data = get_processed_burglary_data()
    try:
        ans[0] = burglary_data.loc[[pinc]]
    except Exception, e:
#        print e
        pass
    try:
        ans[1] = get_persons_pinc_to_df()[pinc]
    except Exception, e:
#        print e
        pass
    try:
        ans[2] = get_property_pinc_to_df()[pinc]
    except Exception, e:
#        print e
        pass
#    print ans
#    pdb.set_trace()
    return ans


def raw_time_to_24hr(raw_time):
    import string
    s = string.split(raw_time.strip(), sep = ' ')
    ampm = s[2]
    time_string = s[1]
    hour = float(string.split(time_string, sep = ':')[0])
    if ampm == 'AM':
        return hour 
    if ampm == 'PM':
        return hour + 12
    assert False


def tong_data_ids():
    f_inc = open(crime_data.constants.tong_fixed_incident_number_file, 'r')
    f_inc.next()
    pos_to_pincnum = [int(line.strip()) for line in f_inc]
    f_inc.close()
    return pos_to_pincnum


def get_tong_patterns():
    tong_ids = tong_data_ids()
    f = open(crime_data.constants.tong_patterns_file, 'r')
    f.next()
    d = {}
    for tong_id, pattern_num in itertools.izip(tong_ids, f):
        d[tong_id] = int(pattern_num.strip())
    return pd.Series(d)


class tong_pattern_id_f(utils.f):

    def __init__(self):
        self.s = get_tong_patterns()

    def __call__(self, tong_id):
        val = self.s[tong_id]
        #print type(val), val, val == 0
        #assert type(val) == int
        if val == 0:
            return None
        else:
            return val


class tong_in_pattern_f(utils.f):

    def __init__(self):
        self.s = get_tong_patterns()

    def __call__(self, tong_id):
        val = self.s[tong_id]
        return not val == 0


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
    merged['tong_id'] = get_tong_patterns()
    print merged.head()
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


class crime_hour_f(utils.f):

    def __repr__(self):
        return 'crime_hour'

    def __init__(self):
        self.d = get_processed_burglary_data()

    def __call__(self, data_id):
        try:
            from_string = self.d.time_from[data_id]
            to_string = self.d.time_to[data_id]
#            print from_string, to_string
            from_hour = raw_time_to_24hr(from_string)
            to_hour = raw_time_to_24hr(to_string)
            if from_hour - .0001 <= to_hour:
                ans = (from_hour + to_hour) / 2.0
            else:
                ans = ((to_hour + 24.0 + from_hour) / 2.0) - 12.0
            assert ans > -0.0001 and ans < 24.00001
            return ans
        except Exception, e:
            pass



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
        from tensor_scan.tensor_scan import fxns as tensor_scan_fxns
        tong_ids = set(tensor_scan_fxns.tong_data_ids())
        self.ids = d.index.to_series()
        self.ids = self.ids[[(data_id in tong_ids) for data_id in self.ids]]

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


def get_pattern_timespans(data):
    id_to_years = {}
    for datum in data:
#        print datum.which_pattern, datum.in_pattern
        if datum.in_pattern:
            try:
                id_to_years[datum.which_pattern].append(datum.time)
            except KeyError:
                id_to_years[datum.which_pattern] = [datum.time]
    for key, val in id_to_years.iteritems():
        id_to_years[key] = sorted(val)

    pattern_list = sorted([(key, val) for (key, val) in id_to_years.iteritems()], key = lambda x:x[1][0])
    return pattern_list
    pdb.set_trace()
    return pd.DataFrame({key:pd.Series([times[0],times[-1], len(times)])} for (key, times) in pattern_list).T
    return pd.Series({key:(times[-1] - times[0], len(times), times[0], times[-1]) for (key, times) in pattern_list})


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
    ax.set_ylim((-1, None))
    fig.suptitle('pattern timeline')
    ax.set_ylabel('pattern index')

    ax.set_title('num_patterns: %d' % len(pattern_list))

    return fig
