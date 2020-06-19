import pytest
import json
import os
import sys
import traceback


############################## Engine :: Configuration Engine :: Environment Configuration ##############################
@pytest.fixture(name="environment_configuration_filename", scope="module")
def _environment_configuration_filename():
    """Test environment config file name."""
    return 'dummy_environment_configuration_filename.json'

@pytest.fixture(name="environment_configuration", scope="module")
def _environment_configuration(test_file_dir):
    """Test environment config."""
    return {
        "DB_ENGINE": "sqlite",
        "DB_FILE": os.path.join(test_file_dir, "clustered.db")
    }

@pytest.yield_fixture(name="environment_configuration_file", scope="module", autouse=True)
def _environment_configuration_file(test_file_dir, environment_configuration_filename, environment_configuration):
    """ 
        Creating JSON file to test environment configuration access.
        The file will be removed once test is completed as part of teardown process.
    """
    try:
        environment_configuration_file = os.path.join(test_file_dir, environment_configuration_filename)
        with open(environment_configuration_file, 'w') as f:
            json.dump(environment_configuration, f)
        yield environment_configuration_file
        os.remove(environment_configuration_file)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False


############################## Engine :: Configuration Engine :: Repository Configuration ##############################
@pytest.fixture(name="repository_configuration_filename", scope="module")
def _repository_configuration_filename():
    """Test repository config file name."""
    return 'dummy_repository_configuration_filename.json'

@pytest.fixture(name="repository_configuration", scope="module")
def _repository_configuration():
    """Test repository config."""
    return {
        "CLOUD_TYPE": "AWS"
    }

@pytest.yield_fixture(name="repository_configuration_file", scope="module", autouse=True)
def _repository_configuration_file(test_file_dir, repository_configuration_filename, repository_configuration):
    """ 
        Creating JSON file to test repository configuration access.
        The file will be removed once test is completed as part of teardown process.
    """
    try:
        repository_configuration_file = os.path.join(test_file_dir, repository_configuration_filename)
        with open(repository_configuration_file, 'w') as f:
            json.dump(repository_configuration, f)
        yield repository_configuration_file
        os.remove(repository_configuration_file)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False
