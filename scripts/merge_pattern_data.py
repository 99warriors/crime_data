import crime_data.constants as constants
import pandas as pd
import pdb
import numpy as np

"""
merges pattern data into format to be read by get_jsoned_data.py
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

files = ['%s/%dPatterns.csv' % (constants.pattern_folder, year) for year in range(1997,2007)]

#dfs = [pandas.read_csv(file, index_col=1) for file in files]
dfs = [pd.read_csv(file, index_col = None) for file in files]



df = pd.concat(dfs, ignore_index=True)
df['pincnum'] = df['File #'].apply(fix_pincnum)

df = df.rename(columns = {'Pattern #':'pattern'})

df = df[[np.isnan(x) == False for x in df.pincnum]]

df.to_csv(constants.merged_pattern_raw_file)

