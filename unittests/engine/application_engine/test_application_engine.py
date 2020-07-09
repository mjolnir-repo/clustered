import pytest
import mock
import json
import pickle
import shutil
import pathlib
import clustered.exceptions as exp
from clustered.engine.application_engine import ApplicationEngine
from clustered.exceptions import ConfigurationFileNotAvailableError,\
                                    ApplicationAlreadyInitiatedError

@pytest.fixture
def app_engine_obj():
    return ApplicationEngine()

@pytest.fixture
def wrkspc():
    return "/home/user"

@pytest.fixture
def dummy_config_file_name():
    return "dummy_config_file.json"

@pytest.fixture
def backup_dummy_config_file_name():
    return "backup_dummy_config_file.json"

@pytest.fixture
def dummy_good_config_data():
    return {
        'APP_NAME': "Mjolnir_Clustered",
        'DB_CONFIG': {
            'DB_ENGINE': "sqlite",
            'DB_FILE': "some_elegant_databse_file.db"
        },
        'USER_CONFIG': [
            {
                'USER_NAME': "saumalya",
                'PASSWORD': "password",
                'ADMIN_FLAG': "Y"
            }
            ,{
                'USER_NAME': "snehik",
                'PASSWORD': "password",
                'ADMIN_FLAG': "N"
            }
        ]
        ,
        'WHITELISTED_IPS': ['1.1.1.1', '0.0.0.0']
    }

def test_get_config_file_user_provided(mocker, app_engine_obj, dummy_config_file_name):
    mocker.patch("pathlib.Path.is_file", return_value=True)
    assert app_engine_obj._get_config_file("DUMMY_ATTR", config_file=dummy_config_file_name) == pathlib.Path(dummy_config_file_name)
    with pytest.raises(ConfigurationFileNotAvailableError):
        mocker.patch("pathlib.Path.is_file", return_value=False)
        assert app_engine_obj._get_config_file("DUMMY_ATTR", config_file=dummy_config_file_name)

def test_get_config_file_environment_declared(mocker, monkeypatch, app_engine_obj, dummy_config_file_name):
    assert not app_engine_obj._get_config_file("DUMMY_ATTR")
    monkeypatch.setenv("CLUSTERED__DUMMY_ATTR_FILE", dummy_config_file_name)
    mocker.patch("pathlib.Path.is_file", return_value=True)
    assert app_engine_obj._get_config_file("DUMMY_ATTR") == pathlib.Path(dummy_config_file_name)
    with pytest.raises(ConfigurationFileNotAvailableError):
        mocker.patch("pathlib.Path.is_file", return_value=False)
        assert app_engine_obj._get_config_file("DUMMY_ATTR")

def test_get_config_file_backup_file(mocker, app_engine_obj, backup_dummy_config_file_name):
    mocker.patch("pathlib.Path.is_file", return_value=True)
    assert app_engine_obj._get_config_file("DUMMY_ATTR", backup_config_file=backup_dummy_config_file_name) == pathlib.Path(backup_dummy_config_file_name)
    with pytest.raises(ConfigurationFileNotAvailableError):
        mocker.patch("pathlib.Path.is_file", return_value=False)
        assert app_engine_obj._get_config_file("DUMMY_ATTR", backup_config_file=backup_dummy_config_file_name)

def test_get_config_file_mandatory_scenario(mocker, app_engine_obj):
    with pytest.raises(ConfigurationFileNotAvailableError):
        assert app_engine_obj._get_config_file("ENVIRON_CONFIG", isinit=True)

def test_set_workspace(mocker, app_engine_obj):
    with pytest.raises(ApplicationAlreadyInitiatedError):
        mocker.patch("builtins.open", return_value=True)
        assert app_engine_obj._setup_wrkspc()
    mocker.patch("pathlib.Path.exists", return_value=False)
    mocker.patch("pathlib.Path.mkdir", return_value=True)
    spy = mocker.spy(pathlib.Path, "mkdir")
    assert app_engine_obj._setup_wrkspc()
    spy.assert_called_once()

def test_build_configuration(mocker, app_engine_obj, wrkspc, dummy_good_config_data):
    config_data = json.dumps(dummy_good_config_data)
    mock_open = mock.mock_open(read_data=config_data)
    with mock.patch("builtins.open", mock_open):
        mocker.patch.object(pickle, "dump", return_value=True)
        assert app_engine_obj._build_configuration(pathlib.Path(wrkspc), 'DUMMY_ATTR', config_data)

def test_setup_meta_db(mocker, app_engine_obj, wrkspc, dummy_good_config_data):
    mocker.patch.object(app_engine_obj, "_get_wrkspc", return_value=pathlib.Path(wrkspc))
    mocker.patch("pathlib.Path.is_file", return_value=True)
    mocker.patch.object(app_engine_obj, "_get_config", return_value=dummy_good_config_data)
    mocker.patch.object(app_engine_obj, "database_url_builder", return_value=dummy_good_config_data['DB_CONFIG']['DB_ENGINE'])
    mocker.patch("sqlalchemy.create_engine", return_value=True)
    mocker.patch("clustered.models.Base.metadata.create_all", return_value=True)
    config_data = json.dumps(dummy_good_config_data)
    mock_open = mock.mock_open(read_data=config_data)
    assert app_engine_obj._setup_meta_db()

def test_initiate(mocker, app_engine_obj, wrkspc, dummy_good_config_data):
    mocker.patch.object(app_engine_obj, "_setup_wrkspc", return_value=pathlib.Path(wrkspc))
    mocker.patch.object(app_engine_obj, "_get_config", return_value=dummy_good_config_data)
    mocker.patch.object(app_engine_obj, "_build_configuration", return_value=True)
    mocker.patch.object(app_engine_obj, "_setup_meta_db", return_value=True)
    assert app_engine_obj.initiate()

def test_refresh(mocker, app_engine_obj, wrkspc, dummy_good_config_data):
    mocker.patch.object(app_engine_obj, "isinitiated", return_value=True)
    mocker.patch.object(app_engine_obj, "_get_wrkspc", return_value=pathlib.Path(wrkspc))
    mocker.patch.object(app_engine_obj, "_get_config", return_value=dummy_good_config_data)
    mocker.patch.object(app_engine_obj, "_build_configuration", return_value=True)
    assert app_engine_obj.refresh()

def test_destroy(mocker, app_engine_obj, wrkspc, dummy_good_config_data):
    mocker.patch.object(app_engine_obj, "isinitiated", return_value=True)
    mocker.patch.object(app_engine_obj, "_get_wrkspc", return_value=pathlib.Path(wrkspc))
    mocker.patch("pathlib.Path.is_file", return_value=True)
    mocker.patch.object(app_engine_obj, "_get_config", return_value=dummy_good_config_data)
    mocker.patch("pathlib.Path.unlink", return_value=True)
    mocker.patch("shutil.rmtree", return_value=True)
    rmtree_spy = mocker.spy(shutil, "rmtree")
    assert app_engine_obj.destroy()
    rmtree_spy.assert_called_once_with(wrkspc)