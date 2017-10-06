import posixpath
import os
import tempfile
import shutil

import pytest

TESTS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = posixpath.join(TESTS_DIR, 'data')

# NB:  This module is magially imported when you run py.test
# and the fixtures below are magically provided to any test function in any test module
# without needing to import them of declare them

@pytest.fixture(scope='session')
def test_data_dir():
    return TEST_DATA_DIR


@pytest.fixture(scope='function')
def temp_dir(request):
    d = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(d, ignore_errors=True)

    request.addfinalizer(fin)
    return d

