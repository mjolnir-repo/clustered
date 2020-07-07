import pytest
import pathlib
import shutil
import os
import traceback
import sys
import json
# from clustered.models import Base
# from clustered.engine.database_engine import DatabaseEngine


@pytest.yield_fixture(name="test_file_dir", scope="module")
def _test_file_dir():
    """
        Creating directory to keep test files.
        The directory will be removed once test is completed as part of teardown process.
    """
    _dir = pathlib.Path.home() / ".test_bed"
    _dir.mkdir(exist_ok=True)
    yield _dir
    shutil.rmtree(_dir.as_posix())

@pytest.yield_fixture(name="test_database_file", scope="module")
def _test_database_file(test_file_dir):
    """
        Creating database file which wil be used through out testing.
        The file will be removed as part of teardown process, once tests are completed.
    """
    db_file = test_file_dir / "default_clustered.db"
    db_file.touch(exist_ok=True)
    yield db_file
    db_file.unlink()

####################################################################################################
################################# Default Environment Configuration ################################
@pytest.fixture(name="default_environment_configuration_filename", scope="module")
def _default_environment_configuration_filename():
    """Test with default environment config file name."""
    return 'Environment_Config.json'

@pytest.fixture(name="default_environment_configuration", scope="module")
def _default_environment_configuration(test_database_file):
    """Test default environment config."""
    return {
        "DB_CONFIG": {
            "DB_ENGINE": "sqlite",
            "DB_FILE": test_database_file.as_posix()
        }
    }

@pytest.yield_fixture(name="default_environment_configuration_file", scope="module", autouse=True)
def _default_environment_configuration_file(default_environment_configuration_filename, default_environment_configuration):
    """ 
        Creating JSON file to test environment configuration access.
        The file will be removed once test is completed as part of teardown process.
    """
    try:
        _dir = pathlib.Path("config")
        _dir.mkdir(exist_ok=True)
        default_environment_configuration_file = _dir / default_environment_configuration_filename
        with open(default_environment_configuration_file.as_posix(), 'w') as f:
            json.dump(default_environment_configuration, f)
        yield default_environment_configuration_file
        default_environment_configuration_file.unlink()
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
        _dir = pathlib.Path("config")
        _dir.mkdir(exist_ok=True)
        default_repository_configuration_file = _dir / default_repository_configuration_filename
        with open(default_repository_configuration_file.as_posix(), 'w') as f:
            json.dump(default_repository_configuration, f)
        yield default_repository_configuration_file
        default_repository_configuration_file.unlink()
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False

####################################################################################################
################################# Dummy database setup for testing #################################

# @pytest.yield_fixture(name="db_engine", scope="module", autouse=True)
# def _db_engine_object(default_environment_configuration_file):
#     """ 
#         Setting up test database.
#         Yielding the db engine object
#         Tearing down the test database
#     """
#     try:
#         db_engine = DatabaseEngine()
#         Base.metadata.create_all(db_engine.engine)
#         yield db_engine
#         Base.metadata.drop_all(db_engine.engine)
#     except Exception as e:
#         print("~" * 100)
#         traceback.print_exc(file=sys.stdout)
#         print("~" * 100)
#         assert False