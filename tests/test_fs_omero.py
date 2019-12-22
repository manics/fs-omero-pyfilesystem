# https://docs.pyfilesystem.org/en/latest/implementers.html#testing-filesystems
from fs.test import FSTestCases
from fs_omero_pyfs import OmeroFS
# import pytest
import unittest
from uuid import uuid4


class TestOmeroFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        # Return an instance of your FS object here
        return OmeroFS(
            host='localhost', user='root', passwd='omero',
            ns=str(uuid4()))
