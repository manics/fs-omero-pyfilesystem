# https://docs.pyfilesystem.org/en/latest/implementers.html#testing-filesystems
from fs.test import FSTestCases
from fs_omero_pyfs import OmeroFS
# import pytest
import unittest
from uuid import uuid4


# Tests are inherited from
# https://github.com/PyFilesystem/pyfilesystem2/blob/v2.4.11/fs/test.py#L248
class TestOmeroFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        # Return an instance of your FS object here
        return OmeroFS(
            host='localhost', user='root', passwd='omero',
            ns=str(uuid4()))
