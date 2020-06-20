import pytest

# Declaring names of different elements of clustered application
@pytest.fixture(name='first_encryptor_name', scope="module")
def _first_encryptor_name():
    return "FIRST_TEST_ENCRYPTOR_VOLATILE"

@pytest.fixture(name='second_encryptor_name', scope="module")
def _second_encryptor_name():
    return "SECOND_TEST_ENCRYPTOR_VOLATILE"


@pytest.fixture(name='first_repository_name', scope="module")
def _first_repository_name():
    return "FIRST_TEST_REPOSITORY_VOLATILE"

@pytest.fixture(name='second_repository_name', scope="module")
def _second_repository_name():
    return "SECOND_TEST_REPOSITORY_VOLATILE"

@pytest.fixture(name='first_aws_key_set', scope="module")
def _first_aws_key_set():
    return {
            'aws_access_key': "AIKAFIRSTAWSACCESSKEYID",
            'aws_secret_key': "#FIRST!AWS$SECRET&ACCESS@KEY%",
            'aws_region': "us-east-1"
        }

@pytest.fixture(name='second_aws_key_set', scope="module")
def _second_aws_key_set():
    return {
            'aws_access_key': "AIKASECONDAWSACCESSKEYID",
            'aws_secret_key': "#SECOND!AWS$SECRET&ACCESS@KEY%",
            'aws_region': "us-east-1"
        }