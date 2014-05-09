import crime_data.constants as constants
import python_utils.python_utils.utils as utils
import pandas
import pdb
import numpy as np

"""
prepares data i will give to visualization, whatever it is
"""

incident = pandas.read_csv(constants.incidents_raw_file, index_col = 0)
burglary = pandas.read_csv(constants.burglary_raw_file, index_col = 0)

incident_burglary = pandas.concat([incident, burglary], axis=1, join='inner')

lat_lngs = incident_burglary['address'].apply(lambda s: utils.get_lat_lng('%s cambridge, MA' % s))
years = incident_burglary['date_from'].apply(utils.date_string_to_year)

patterns = pandas.read_csv(constants.merged_pattern_raw_file, index_col = 0)

def fix_pincnum(s):
    try:
        import re
        if pandas.isnull(s):
            return s
        s = re.sub('\-', '0', s)
        s = str(int(float(s)))
        if len(s) == 9:
            return int('%s0%s' % (s[0:4], s[5:]))
        else:
            return int(s)
    except ValueError:
        return np.nan

patterns['pincnum'] = pandas.Series(patterns.index, index=patterns.index).apply(fix_pincnum)
patterns.index = patterns['pincnum']


incident_burglary['lat_lng'] = lat_lngs
incident_burglary['year'] = years

#incident_burglary.to_json(constants.burglary_json_out_file, orient='index')

def f(df):
    print df.shape
    return df.shape[0]



patterns = patterns[['pattern']]
merged = pandas.merge(patterns, incident_burglary, how='right', right_index=True, left_index = True)


pdb.set_trace()

#incident_burglary.to_json(constants.burglary_json_out_file, orient='index')



merged.to_json(constants.burglary_json_out_file, orient='index')
merged.to_csv(constants.burglary_csv_out_file, index=False)




