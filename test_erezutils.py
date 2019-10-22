import hashlib
import io
import unittest
import uuid

import boto3
from botocore.exceptions import NoCredentialsError
from erezutils import chunks, delete_from_s3, hashfiles, list_s3_bucket_keys, rupdate

BUCKET = "travisci-test-bucket"


class RecursiveUpdateTest(unittest.TestCase):
    def test_update_empty(self):
        self.assertEqual({"a": 1}, rupdate({}, {"a": 1}))

    def test_update_existing(self):
        self.assertEqual({"a": 2}, rupdate({"a": 1}, {"a": 2}))

    def test_update_with_other_keys(self):
        self.assertEqual({"a": 2, "b": 1}, rupdate({"a": 1}, {"a": 2, "b": 1}))

    def test_update_leaves_other_keys_untouched(self):
        self.assertEqual({"a": 2, "b": 1}, rupdate({"a": 1, "b": 1}, {"a": 2}))

    def test_recursive_update(self):
        self.assertEqual({"a": {"b": 2}}, rupdate({"a": {"b": 1}}, {"a": {"b": 2}}))

    def test_recursive_update_on_empty_dict(self):
        self.assertEqual({"a": {"b": 2}}, rupdate({}, {"a": {"b": 2}}))

    def test_recursive_update_with_other_keys(self):
        self.assertEqual(
            {"a": {"b": 2, "c": 1}}, rupdate({"a": {"b": 1}}, {"a": {"b": 2, "c": 1}})
        )

    def test_recursive_update_leaves_other_keys_untouched(self):
        self.assertEqual(
            {"a": {"b": 2, "c": 1}}, rupdate({"a": {"b": 1, "c": 1}}, {"a": {"b": 2}})
        )


class HashfilesTest(unittest.TestCase):
    def test_hash_default_sha512(self):
        data = b"thisisatest"
        data_sha512 = hashlib.sha512()
        data_sha512.update(data)

        f = io.BytesIO(data)
        self.assertEqual(data_sha512.hexdigest(), hashfiles([f]))

    def test_hash_sha256(self):
        data = b"thisisatest"
        data_sha256 = hashlib.sha256()
        data_sha256.update(data)

        f = io.BytesIO(data)
        self.assertEqual(data_sha256.hexdigest(), hashfiles([f], algorithm="sha256"))

    def test_hash_multiple_files(self):
        data1 = b"thisisatest"
        data2 = b"andanothertest"
        data_sha512 = hashlib.sha512()
        data_sha512.update(data1)
        data_sha512.update(data2)

        f1 = io.BytesIO(data1)
        f2 = io.BytesIO(data2)
        self.assertEqual(data_sha512.hexdigest(), hashfiles([f1, f2]))


class ChunksTest(unittest.TestCase):
    def test_empty(self):
        data = []
        self.assertEqual([], list(chunks(data, 1)))

    def test_one_chunk(self):
        data = [1]
        self.assertEqual([[1]], list(chunks(data, 1)))

    def test_chuck_group_1(self):
        data = [1, 2]
        self.assertEqual([[1], [2]], list(chunks(data, 1)))

    def test_chunk_group_2(self):
        data = [1, 2, 3, 4]
        self.assertEqual([[1, 2], [3, 4]], list(chunks(data, 2)))


def test_bucket_access():
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(BUCKET)
    try:
        # Contact S3.
        bucket.creation_date
    except NoCredentialsError:
        return True
    return False


# https://docs.travis-ci.com/user/pull-requests/#pull-requests-and-security-restrictions
@unittest.skipIf(
    test_bucket_access(), "Access to AWS credentials forbidden on pull requests"
)
class S3OperationsTest(unittest.TestCase):
    # Prevent conflicts between concurrent test runs.
    PREFIX = str(uuid.uuid4())

    def setUp(self):
        super().setUp()
        self.s3_client = boto3.client("s3")

    def tearDown(self):
        remaining_keys = self.list_keys()
        delete_from_s3(self.s3_client, BUCKET, remaining_keys)
        super().tearDown()

    def make_key(self, filename):
        return self.PREFIX + filename

    def create_file(self, filename):
        key = self.make_key(filename)
        self.s3_client.put_object(Bucket=BUCKET, Key=key)

    def list_keys(self):
        return list_s3_bucket_keys(self.s3_client, BUCKET, Prefix=self.PREFIX)

    def test_list_file(self):
        filename = "a_file"
        self.create_file(filename)
        self.assertEqual([self.make_key(filename)], self.list_keys())

    def test_list_nested_files(self):
        file1 = "dir1/f1"
        file2 = "dir2/f1"
        self.create_file(file1)
        self.create_file(file2)
        self.assertEqual([self.make_key(file1), self.make_key(file2)], self.list_keys())

    def test_clean_files(self):
        filename = "a_file"
        self.create_file(filename)
        delete_from_s3(self.s3_client, BUCKET, [self.make_key(filename)])
        self.assertEqual([], self.list_keys())
