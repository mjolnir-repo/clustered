import pytest
import mock
import json
import pathlib
import clustered.exceptions as exp
from clustered.engine.base_engine import ApplicationBase
from clustered.exceptions import ApplicationNotInitiatedError\
                                , UnexpectedSystemError\
                                , ConfigurationNotAvailableError\
                                , ApplicationDatabaseUnsupportedError

@pytest.fixture
def app_base_obj():
    return ApplicationBase()

@pytest.fixture
def dummy_config_keys():
    return [
        {
            'name': "APP_NAME",
            'type': 'str'
        }
        ,{
            'name': "DB_CONFIG",
            'type': 'dict',
            'child': [{
                'name': "DB_ENGINE",
                'type': 'str'
            }]
        }
        ,{
            'name': "USER_CONFIG",
            'type': 'list',
            'elem_type': 'dict',
            'elem_child': [{
                'name': "USER_NAME",
                'type': 'str'
            },{
                'name': "PASSWORD",
                'type': 'str'
            },{
                'name': "ADMIN_FLAG",
                'type': 'str'
            }]
        }
        ,{
            'name': "WHITELISTED_IPS",
            'type': 'list',
            'elem_type': 'str'
        }
    ]

@pytest.fixture
def dummy_good_config_data():
    return {
        'APP_NAME': "Mjolnir_Clustered",
        'DB_CONFIG': {
            'DB_ENGINE': "sqlite"
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

@pytest.fixture
def dummy_wrong_config_data():
    return {
        'APP_NAME': "Mjolnir_Clustered",
        'DB_CONFIG': {
            'DB_ENGINE': "sqlite"
        },
        'USER_CONFIG': [
            {
                'USER_NAME': "saumalya",
                'PASSWORD': "password"
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

@pytest.fixture
def dummy_bad_config_keys_type_1():
    return [
        {
            'name': "APP_NAME",
            'type': 'bad'
        }
        ,{
            'name': "DB_CONFIG",
            'type': 'dict',
            'child': [{
                'name': "DB_ENGINE",
                'type': 'str'
            }]
        }
        ,{
            'name': "USER_CONFIG",
            'type': 'list',
            'elem_type': 'dict',
            'elem_child': [{
                'name': "USER_NAME",
                'type': 'str'
            },{
                'name': "PASSWORD",
                'type': 'str'
            },{
                'name': "ADMIN_FLAG",
                'type': 'str'
            }]
        }
        ,{
            'name': "WHITELISTED_IPS",
            'type': 'list',
            'elem_type': 'str'
        }
    ]

@pytest.fixture
def dummy_bad_config_keys_type_2():
    return [
        {
            'name': "APP_NAME",
            'type': 'str'
        }
        ,{
            'name': "DB_CONFIG",
            'type': 'dict',
            'child': [{
                'name': "DB_ENGINE",
                'type': 'str'
            }]
        }
        ,{
            'name': "USER_CONFIG",
            'type': 'list',
            'elem_type': 'bad',
            'elem_child': [{
                'name': "USER_NAME",
                'type': 'str'
            },{
                'name': "PASSWORD",
                'type': 'str'
            },{
                'name': "ADMIN_FLAG",
                'type': 'str'
            }]
        }
        ,{
            'name': "WHITELISTED_IPS",
            'type': list,
            'elem_type': str
        }
    ]

def test_application_initiated(mocker, app_base_obj):
    mocker.patch("pathlib.Path.is_dir", return_value=True)
    assert app_base_obj.isinitiated()

def test_application_not_initiated(mocker, app_base_obj):
    mocker.patch("pathlib.Path.is_dir", return_value=False)
    with pytest.raises(exp.ApplicationNotInitiatedError):
        assert app_base_obj.isinitiated()

def test_get_workspace(app_base_obj):
    assert app_base_obj._get_wrkspc().as_posix() == (pathlib.Path.home() / '.clustered').as_posix()

def test_recursive_key_checker_good_config(app_base_obj, dummy_config_keys, dummy_good_config_data):
    result = app_base_obj._recursive_key_checker(dummy_config_keys, dummy_good_config_data)
    assert result[0]

def test_recursive_key_checker_bad_config(app_base_obj, dummy_config_keys, dummy_wrong_config_data):
    result = app_base_obj._recursive_key_checker(dummy_config_keys, dummy_wrong_config_data)
    assert not result[0]

def test_recursive_key_checker_bad_config_key_type_1(app_base_obj, dummy_bad_config_keys_type_1, dummy_good_config_data):
    with pytest.raises(UnexpectedSystemError) as e:
        app_base_obj._recursive_key_checker(dummy_bad_config_keys_type_1, dummy_good_config_data)
        assert str(e) == "You-Know-who is back!!"

def test_recursive_key_checker_bad_config_key_type_2(app_base_obj, dummy_bad_config_keys_type_2, dummy_good_config_data):
    with pytest.raises(UnexpectedSystemError) as e:
        app_base_obj._recursive_key_checker(dummy_bad_config_keys_type_2, dummy_good_config_data)
        assert str(e) == "You-Know-who is back!!"

@pytest.mark.parametrize("attr, key", [
    ('ENVIRON_CONFIG', 'MANDATORY_ENVIRON_CONFIG_KEYS'),
    ('REPOSITORY_CONFIG', 'MANDATORY_REPOSITORY_CONFIG_KEYS'),
    ('CLUSTER_CONFIG', 'MANDATORY_CLUSTER_CONFIG_KEYS'),
    ('PARENT_NODE_CONFIG', 'MANDATORY_PARENT_NODE_CONFIG_KEYS'),
    ('CHILD_NODE_CONFIG', 'MANDATORY_CHILD_NODE_CONFIG_KEYS')
])
def test_check_for_mandatory_config(mocker, attr, key, app_base_obj, dummy_good_config_data):
    config_data = json.dumps(dummy_good_config_data)
    mock_open = mock.mock_open(read_data=config_data)
    with mock.patch("builtins.open", mock_open):
        mocker.patch.object(app_base_obj, "_recursive_key_checker", return_value=(True, dummy_good_config_data))
        spy = mocker.spy(app_base_obj, "_recursive_key_checker")
        assert app_base_obj._check_for_mandatory_config(attr, 'random_file_name.txt')
        spy.assert_called_once_with(getattr(app_base_obj, key), dummy_good_config_data)
        assert spy.spy_return == (True, dummy_good_config_data)

@pytest.mark.parametrize("attr, config_file", [
    ('ENVIRON_CONFIG', 'env_config_file.json'),
    ('REPOSITORY_CONFIG', 'repository_config_file.json')
])
def test_get_config(mocker, attr, config_file, app_base_obj, dummy_good_config_data):
    mocker.patch.object(app_base_obj, "_get_config_file", return_value=pathlib.Path(config_file))
    mocker.patch.object(app_base_obj, "_check_for_mandatory_config", return_value=(True, {'a': 1}))
    get_file_config_spy = mocker.spy(app_base_obj, "_get_config_file")
    check_for_mandatory_config_spy = mocker.spy(app_base_obj, "_check_for_mandatory_config")
    assert app_base_obj._get_config(attr, config_file) == {'a': 1}
    get_file_config_spy.assert_called_once_with(attr, config_file, "", False)
    check_for_mandatory_config_spy.assert_called_once_with(attr, config_file)

@pytest.mark.parametrize("attr, config_file", [
    ('ENVIRON_CONFIG', 'env_config_file.json'),
    ('REPOSITORY_CONFIG', 'repository_config_file.json')
])
def test_get_config_config_file_not_available(mocker, attr, config_file, app_base_obj):
    mocker.patch.object(app_base_obj, "_get_config_file", return_value=False)
    assert app_base_obj._get_config(attr, config_file) == {}

def test_database_url_builder(app_base_obj):
    env_config = {'DB_CONFIG': {'DB_ENGINE': 'sqlite', 'DB_FILE': 'some/random/db/file.db'}}
    assert app_base_obj.database_url_builder(env_config) == 'sqlite:///{}'.format(env_config['DB_CONFIG']['DB_FILE'])

@pytest.mark.parametrize("env_config, excp", [
    ({'DB_CONFIG': {'DB_ENGINE': 'postgres'}}, ApplicationDatabaseUnsupportedError),
    ({'DB_CONFIG': {'DB_ENGINE': 'sqlite'}}, ConfigurationNotAvailableError),
])
def test_database_url_builder_errors(app_base_obj, env_config, excp):
    with pytest.raises(excp):
        assert app_base_obj.database_url_builder(env_config) == 'sqlite:///{}'.format(env_config['DB_CONFIG']['DB_FILE'])

