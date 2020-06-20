import pytest

# Declaring names of different elements of clustered application
@pytest.fixture(name='first_encryptor_name', scope="module")
def _first_encryptor_name():
    return "FIRST_TEST_ENCRYPTOR_VOLATILE"

@pytest.fixture(name='second_encryptor_name', scope="module")
def _second_encryptor_name():
    return "SECOND_TEST_ENCRYPTOR_VOLATILE"