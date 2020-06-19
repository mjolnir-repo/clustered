import pytest
import json
import traceback
import sys
import os
from clustered.exceptions import ConfigurationNotAvailableError
from clustered.engine.configuration_engine import engine
from clustered.engine.configuration_engine import EnvironmentConfiguration


# Declaring the fixtures specific to this test file
@pytest.fixture(name='derived_configurations', scope="module")
def _derived_configurations():
    """Derived Configuration Keys."""
    return ["DATABASE_URL"]

@pytest.fixture(name="incomplete_environment_configuration_filename", scope="module")
def _incomplete_environment_configuration_filename():
    """Incomplete environment config file name for testing."""
    return 'dummy_incomplete_environment_configuration_filename.json'

@pytest.fixture(name="incomplete_environment_configuration", scope="module")
def _incomplete_environment_configuration():
    """Incomplete environment config for testing."""
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
def test_configuration_via_user_provided_file(test_file_dir, environment_configuration_file, derived_configurations):
    """ Testing configuration extraction from proper configuration file. """
    env_config = engine.env(environment_configuration_file)
    assert list(env_config.keys()) == EnvironmentConfiguration.MANDATORY_CONFIG_KEY + derived_configurations
    assert env_config['DB_FILE'] == os.path.join(test_file_dir, "clustered.db")
    assert env_config['DATABASE_URL'] == 'sqlite:///{}'.format(env_config['DB_FILE'])

def test_configuration_via_environemnt_declared_incomplete_file(test_file_dir, incomplete_environment_configuration_file, derived_configurations):
    """ Testing configuration extraction from incomplete configuration file. """
    os.environ["CLUSTERED__ENVIRON_CONFIG_FILE"] = incomplete_environment_configuration_file
    os.environ["CLUSTERED__DB_FILE"] = os.path.join(test_file_dir, "incomplete_clustered.db")
    env_config = engine.env(refresh=True)
    assert list(env_config.keys()) == EnvironmentConfiguration.MANDATORY_CONFIG_KEY + derived_configurations
    assert env_config['DB_FILE'] == os.path.join(test_file_dir, "incomplete_clustered.db")
    assert env_config['DATABASE_URL'] == 'sqlite:///{}'.format(env_config['DB_FILE'])
    os.environ["CLUSTERED__ENVIRON_CONFIG_FILE"] = ""
    os.environ["CLUSTERED__DB_FILE"] = ""

def test_configuration_via_default_file(test_file_dir, default_environment_configuration_filename, derived_configurations):
    """ Testing configuration extraction from default configuration file. """
    env_config = engine.env(refresh=True)
    assert list(env_config.keys()) == EnvironmentConfiguration.MANDATORY_CONFIG_KEY + derived_configurations
    assert env_config['DB_FILE'] == os.path.join(test_file_dir, "default_clustered.db")
    assert env_config['DATABASE_URL'] == 'sqlite:///{}'.format(env_config['DB_FILE'])

def test_configuration_not_available_error(incomplete_environment_configuration_file, derived_configurations):
    """ Testing configuration extraction from default configuration file. """
    with pytest.raises(ConfigurationNotAvailableError):
        assert list(engine.env(incomplete_environment_configuration_file).keys()) == EnvironmentConfiguration.MANDATORY_CONFIG_KEY + derived_configurations

def test_singletone(repository_configuration_file):
    repo_config_one = engine.repo(repository_configuration_file)
    repo_config_two = engine.repo()
    assert repo_config_one == repo_config_two
    repo_config_three = engine.repo(refresh=True)
    with pytest.raises(AssertionError):
        assert repo_config_one == repo_config_three
