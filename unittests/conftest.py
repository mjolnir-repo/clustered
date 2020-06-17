import pytest
import json
import os
import sys
import traceback


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


############################## Engine :: Configuration Engine :: Environment Configuration ##############################
@pytest.fixture(name="environment_configuration_filename", scope="module")
def _environment_configuration_filename():
    """Test environment config file name."""
    return 'dummy_environment_configuration_filename.json'

@pytest.fixture(name="environment_configuration", scope="module")
def _environment_configuration():
    """Test environment config."""
    return {
        "DB_ENGINE": "sqlite",
        "DB_FILE": "/Volumes/WorkSpace/POC/Mjolnir/clustered/clustered/database/clustered.db",
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

@pytest.fixture(name="default_environment_configuration_filename", scope="module")
def _default_environment_configuration_filename():
    """Test with default environment config file name."""
    return 'Environment_Config.json'

@pytest.fixture(name="default_environment_configuration", scope="module")
def _default_environment_configuration():
    """Test default environment config."""
    return {
        "DB_ENGINE": "sqlite",
        "DB_FILE": "/Volumes/WorkSpace/POC/Mjolnir/clustered/clustered/database/default_clustered.db",
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


############################## Engine :: Configuration Engine :: Repository Configuration ##############################
@pytest.fixture(name="repository_configuration_filename", scope="module")
def _repository_configuration_filename():
    """Test repository config file name."""
    return 'dummy_repository_configuration_filename.json'

@pytest.fixture(name="repository_configuration", scope="module")
def _repository_configuration():
    """Test repository config."""
    return {
        "AWS_ACCESS_KEY_ID": "TEST",
        "AWS_SECRET_ACCESS_KEY": "TEST",
        "AWS_DEFAULT_REGION": "TEST"
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

@pytest.fixture(name="default_repository_configuration_filename", scope="module")
def _default_repository_configuration_filename():
    """Test with default repository config file name."""
    return 'Repository_Config.json'

@pytest.fixture(name="default_repository_configuration", scope="module")
def _default_repository_configuration():
    """Test default repository config."""
    return {
        "AWS_ACCESS_KEY_ID": "dEFAULTfILEtEST",
        "AWS_SECRET_ACCESS_KEY": "TEST",
        "AWS_DEFAULT_REGION": "TEST"
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