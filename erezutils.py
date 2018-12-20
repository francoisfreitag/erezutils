try:
    from collections.abc import Mapping
except ImportError:
    # Python 2.7
    from collections import Mapping
import hashlib


def rupdate(d, u):
    """Recursively update dictionary d with the contents of u."""
    for k, v in u.items():
        if isinstance(v, Mapping):
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


def delete_from_s3(client, bucket, keys):
    if keys:
        # Can only delete up to 1000 keys per request.
        # https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.delete_objects
        for batch in chunks(keys, 1000):
            delete_keys = {"Objects": [{"Key": key} for key in batch]}
            client.delete_objects(Bucket=bucket, Delete=delete_keys)


def list_s3_bucket_keys(client, bucket, **kwargs):
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, **kwargs):
        if "Contents" in page:
            keys.extend(object["Key"] for object in page["Contents"])
    return keys
