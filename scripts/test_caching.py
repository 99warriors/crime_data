import crime_pattern.constants as constants
import crime_pattern.fxns.caching as caching

method_read_dec = caching.method_read_dec(caching.read_pickle, caching.generic_get_path, 'pickle')
method_write_dec = caching.method_write_dec(caching.write_pickle, caching.generic_get_path, 'pickle')
method_cache_dec = caching.method_cache_dec(caching.generic_get_arg_key)

fxn_read_dec = caching.fxn_read_dec(caching.read_pickle, caching.generic_get_path, 'pickle')
fxn_write_dec = caching.fxn_write_dec(caching.write_pickle, caching.generic_get_path, 'pickle')
fxn_cache_dec = caching.fxn_cache_dec(caching.generic_get_arg_key)

class c(object):

    @method_cache_dec
    @method_read_dec
    @method_write_dec
    def g(self, x):
        print 'OH SHIT'
        return x*x


@fxn_cache_dec
@fxn_read_dec
@fxn_write_dec
def gg(x):
    print 'OH FUDGE'
    return x**3


print gg(5)
print gg(5)

print c().g(3)
print c().g(3)

class h(object):

    def __init__(self, ff):
        self.ff = ff
import pickle, pdb
asdf = h(c())
pickle.dumps(asdf)
pdb.set_trace()
