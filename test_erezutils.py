import hashlib
import io
import unittest

from erezutils import chunks, hashfiles, rupdate


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
        self.assertEqual({
            "a": {"b": 2}},
            rupdate({"a": {"b": 1}}, {"a": {"b": 2}})
        )

    def test_recursive_update_on_empty_dict(self):
        self.assertEqual({"a": {"b": 2}}, rupdate({}, {"a": {"b": 2}}))

    def test_recursive_update_with_other_keys(self):
        self.assertEqual(
            {"a": {"b": 2, "c": 1}},
            rupdate({"a": {"b": 1}}, {"a": {"b": 2, "c": 1}})
        )

    def test_recursive_update_leaves_other_keys_untouched(self):
        self.assertEqual(
            {"a": {"b": 2, "c": 1}},
            rupdate({"a": {"b": 1, "c": 1}}, {"a": {"b": 2}})
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
        self.assertEqual(
            data_sha256.hexdigest(), hashfiles([f], algorithm="sha256")
        )

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
