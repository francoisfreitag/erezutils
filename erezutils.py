import collections
import hashlib


def rupdate(d, u):
    """Recursively update dictionary d with the contents of u."""
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            v = rupdate(d.get(k, {}), v)
        d[k] = v
    return d


def hashfiles(fps, algorithm='sha512'):
    h = hashlib.new(algorithm)
    for fp in fps:
        h.update(fp.read())
    return h.hexdigest()


def chunks(lst, n):
    """Yield successive n-sized iterators from list lst."""
    return (lst[i:(i + n)] for i in range(0, len(lst), n))
