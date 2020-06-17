import pytest
import json
import traceback
import sys
import os
from clustered.exceptions import ConfigurationNotAvailableError
from clustered.engine.configuration_engine import engine
from clustered.engine.configuration_engine import EnvironmentConfiguration


# Declaring the fixtures specific to this test file
@pytest.fixture(name="incomplete_environment_configuration_filename", scope="module")
def _incomplete_environment_configuration_filename():
    """Test with incomplete environment config file name."""
    return 'dummy_incomplete_environment_configuration_filename.json'

@pytest.fixture(name="incomplete_environment_configuration", scope="module")
def _incomplete_environment_configuration():
    """Test incomplete environment config."""
    return {
        "DB_ENGINE": "sqlite"
    }

@pytest.yield_fixture(name="incomplete_environment_configuration_file", scope="module", autouse=True)
def _incomplete_environment_configuration_file(test_file_dir, incomplete_environment_configuration_filename, incomplete_environment_configuration):
    """ 
        Creating JSON file to test environment configuration access.
        The file will be removed once test is completed as part of teardown process.
    """
    try:
        incomplete_environment_configuration_file = os.path.join(test_file_dir, incomplete_environment_configuration_filename)
        with open(incomplete_environment_configuration_file, 'w') as f:
            json.dump(incomplete_environment_configuration, f)
        yield incomplete_environment_configuration_file
        os.remove(incomplete_environment_configuration_file)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False


# Declaring the tests
def test_configuration_via_user_provided_file(environment_configuration_file):
    """ Testing configuration extraction from proper configuration file. """
    env_config_obj = engine.env(environment_configuration_file)
    assert env_config_obj.__class__.__name__ == 'EnvironmentConfiguration'
    env_config = env_config_obj()
    assert env_config['DB_FILE'] == "/Volumes/WorkSpace/POC/Mjolnir/clustered/clustered/database/clustered.db"
    assert env_config['DATABASE_URL'] == 'sqlite:///{}'.format(env_config['DB_FILE'])

def test_configuration_via_environemnt_declared_incomplete_file(incomplete_environment_configuration_file):
    """ Testing configuration extraction from incomplete configuration file. """
    os.environ["CLUSTERED__ENVIRON_CONFIG_FILE"] = incomplete_environment_configuration_file
    os.environ["CLUSTERED__DB_FILE"] = "/Volumes/WorkSpace/POC/Mjolnir/clustered/clustered/database/incomplete_clustered.db"
    env_config_obj = engine.env()
    assert env_config_obj.__class__.__name__ == 'EnvironmentConfiguration'
    env_config = env_config_obj()
    assert env_config['DB_FILE'] == "/Volumes/WorkSpace/POC/Mjolnir/clustered/clustered/database/incomplete_clustered.db"
    assert env_config['DATABASE_URL'] == 'sqlite:///{}'.format(env_config['DB_FILE'])
    os.environ["CLUSTERED__ENVIRON_CONFIG_FILE"] = ""
    os.environ["CLUSTERED__DB_FILE"] = ""

def test_configuration_via_default_file(default_environment_configuration_filename):
    """ Testing configuration extraction from default configuration file. """
    env_config_obj = engine.env()
    assert env_config_obj.__class__.__name__ == 'EnvironmentConfiguration'
    env_config = env_config_obj()
    assert env_config['DB_FILE'] == "/Volumes/WorkSpace/POC/Mjolnir/clustered/clustered/database/default_clustered.db"
    assert env_config['DATABASE_URL'] == 'sqlite:///{}'.format(env_config['DB_FILE'])

def test_configuration_not_available_error(incomplete_environment_configuration_file):
    """ Testing configuration extraction from default configuration file. """
    with pytest.raises(ConfigurationNotAvailableError):
        assert engine.env(incomplete_environment_configuration_file).__class__.__name__ == 'EnvironmentConfiguration'
