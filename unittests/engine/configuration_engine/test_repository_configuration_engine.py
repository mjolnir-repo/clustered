import pytest
import json
import traceback
import sys
import os
from clustered.exceptions import ConfigurationNotAvailableError, ConfigurationFileNotAvailableError
from clustered.engine.configuration_engine import engine
from clustered.engine.configuration_engine import RepositoryConfiguration


# Declaring the fixtures specific to this test file
@pytest.fixture(name="incomplete_repository_configuration_filename", scope="module")
def _incomplete_repository_configuration_filename():
    """Test with incomplete repository config file name."""
    return 'dummy_incomplete_repository_configuration_filename.json'

@pytest.fixture(name="incomplete_repository_configuration", scope="module")
def _incomplete_repository_configuration():
    """Test incomplete repository config."""
    return {}

@pytest.yield_fixture(name="incomplete_repository_configuration_file", scope="module", autouse=True)
def _incomplete_repository_configuration_file(test_file_dir, incomplete_repository_configuration_filename, incomplete_repository_configuration):
    """ 
        Creating JSON file to test repository configuration access.
        The file will be removed once test is completed as part of teardown process.
    """
    try:
        incomplete_repository_configuration_file = os.path.join(test_file_dir, incomplete_repository_configuration_filename)
        with open(incomplete_repository_configuration_file, 'w') as f:
            json.dump(incomplete_repository_configuration, f)
        yield incomplete_repository_configuration_file
        os.remove(incomplete_repository_configuration_file)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False


# Declaring the tests
def test_configuration_via_user_provided_file(repository_configuration_file):
    """ Testing repository configuration extraction from proper configuration file. """
    repo_config = engine.repo(repository_configuration_file)
    assert list(repo_config.keys()) == RepositoryConfiguration.MANDATORY_CONFIG_KEY
    assert repo_config['CLOUD_TYPE'] == "AWS"

def test_configuration_via_environemnt_declared_incomplete_file(incomplete_repository_configuration_file):
    """ Testing repository configuration extraction from incomplete configuration file. """
    os.environ["CLUSTERED__REPOSITORY_CONFIG_FILE"] = incomplete_repository_configuration_file
    os.environ["CLUSTERED__CLOUD_TYPE"] = "AWS-ENV"
    repo_config = engine.repo()
    assert list(repo_config.keys()) == RepositoryConfiguration.MANDATORY_CONFIG_KEY
    assert repo_config['CLOUD_TYPE'] == "AWS-ENV"
    os.environ["CLUSTERED__REPOSITORY_CONFIG_FILE"] = ""
    os.environ["CLUSTERED__CLOUD_TYPE"] = ""

def test_configuration_via_default_file(default_repository_configuration_filename):
    """ Testing repository configuration extraction from default configuration file. """
    repo_config = engine.repo()
    assert list(repo_config.keys()) == RepositoryConfiguration.MANDATORY_CONFIG_KEY
    assert repo_config['CLOUD_TYPE'] == "AWS-DEFAULT"

# def test_configuration_file_not_available_error():
#     """ Testing raised exception if mandatory configurations are not present. """
#     with pytest.raises(ConfigurationFileNotAvailableError):
#         repo_config = engine.repo()
#         assert list(repo_config.keys()) == RepositoryConfiguration.MANDATORY_CONFIG_KEY

def test_configuration_not_available_error(incomplete_repository_configuration_file):
    """ Testing raised exception if mandatory configurations are not present. """
    with pytest.raises(ConfigurationNotAvailableError):
        assert list(engine.repo(incomplete_repository_configuration_file).keys()) == RepositoryConfiguration.MANDATORY_CONFIG_KEY
