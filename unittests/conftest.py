import pytest
import os
import traceback
import sys
import json

@pytest.yield_fixture(name="test_file_dir", scope="module")
def _test_file_dir():
    """
        Creating directory to keep test files.
        The directory will be removed once test is completed as part of teardown process.
    """
    _dir = ".test_bed"
    if not os.path.exists(_dir):
        os.mkdir(_dir)
    yield ".test_bed"
    os.rmdir(_dir)


####################################################################################################
################################# Default Environment Configuration ################################
@pytest.fixture(name="default_environment_configuration_filename", scope="module")
def _default_environment_configuration_filename():
    """Test with default environment config file name."""
    return 'Environment_Config.json'

@pytest.fixture(name="default_environment_configuration", scope="module")
def _default_environment_configuration(test_file_dir):
    """Test default environment config."""
    return {
        "DB_ENGINE": "sqlite",
        "DB_FILE": os.path.join(test_file_dir, "default_clustered.db")
    }

@pytest.yield_fixture(name="default_environment_configuration_file", scope="module", autouse=True)
def _default_environment_configuration_file(default_environment_configuration_filename, default_environment_configuration):
    """ 
        Creating JSON file to test environment configuration access.
        The file will be removed once test is completed as part of teardown process.
    """
    try:
        _dir = "config"
        if not os.path.exists(_dir):
            os.mkdir(_dir)
        default_environment_configuration_file = os.path.join("config", default_environment_configuration_filename)
        with open(default_environment_configuration_file, 'w') as f:
            json.dump(default_environment_configuration, f)
        yield default_environment_configuration_file
        os.remove(default_environment_configuration_file)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False

####################################################################################################
################################# Default Repository Configuration #################################
@pytest.fixture(name="default_repository_configuration_filename", scope="module")
def _default_repository_configuration_filename():
    """Test with default repository config file name."""
    return 'Repository_Config.json'

@pytest.fixture(name="default_repository_configuration", scope="module")
def _default_repository_configuration():
    """Test default repository config."""
    return {
        "CLOUD_TYPE": "AWS-DEFAULT"
    }

@pytest.yield_fixture(name="default_repository_configuration_file", scope="module", autouse=True)
def _default_repository_configuration_file(default_repository_configuration_filename, default_repository_configuration):
    """ 
        Creating JSON file to test repository configuration access.
        The file will be removed once test is completed as part of teardown process.
    """
    try:
        _dir = "config"
        if not os.path.exists(_dir):
            os.mkdir(_dir)
        default_repository_configuration_file = os.path.join("config", default_repository_configuration_filename)
        with open(default_repository_configuration_file, 'w') as f:
            json.dump(default_repository_configuration, f)
        yield default_repository_configuration_file
        os.remove(default_repository_configuration_file)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False