import sys
import json
import pickle
import typing
import pathlib
import traceback
from os.path import expanduser
from clustered.exceptions import ApplicationNotInitiatedError\
                                , UnexpectedSystemError\
                                , ConfigurationNotAvailableError\
                                , ApplicationDatabaseUnsupportedError

class ApplicationBase:
    MANDATORY_ENVIRON_CONFIG_KEYS = [
        {
            'name': "DB_CONFIG",
            'type': 'dict',
            'child': [{
                'name': "DB_ENGINE",
                'type': 'str'
            }]
        }
        # ,{
        #     'name': "USER_CONFIG",
        #     'type': list,
        #     'elem_type': dict,
        #     'elem_child': [{
        #         'name': "USER_NAME",
        #         'type': str
        #     },{
        #         'name': "PASSWORD",
        #         'type': str
        #     },{
        #         'name': "ADMIN_FLAG",
        #         'type': str
        #     }]
        # }
    ]
    MANDATORY_REPOSITORY_CONFIG_KEYS = [
        {
            'key': "CLOUD_TYPE",
            'type': 'str'
        }
    ]
    MANDATORY_CLUSTER_CONFIG_KEYS = []
    MANDATORY_PARENT_NODE_CONFIG_KEYS = []
    MANDATORY_CHILD_NODE_CONFIG_KEYS = []

    @staticmethod
    def isinitiated() -> bool:
        wrkspc = pathlib.Path.home() / ".clustered"
        if not wrkspc.is_dir():
            raise ApplicationNotInitiatedError()
        return True

    @staticmethod
    def _get_wrkspc() -> pathlib.Path:
        return pathlib.Path.home() / ".clustered"

    def _recursive_key_checker(self, config_keys: list, config_data: dict) -> typing.Tuple[bool, str]:
        try:
            for key in config_keys:
                if key['name'] not in config_data:
                    return (False, key['name'])
                else:
                    if key['type'] == 'str':
                        if type(config_data[key['name']]) not in [str, int, bool]:
                            return (False, key['name'])
                    elif key['type'] == 'list':
                        if type(config_data[key['name']]) != list:
                            return (False, key['name'])
                        else:
                            if key['elem_type'] == 'str':
                                for value in config_data[key['name']]:
                                    if type(value) not in [str, int, bool]:
                                        return (False, key['name'])
                            elif key['elem_type'] == 'dict':
                                for value in config_data[key['name']]:
                                    result = self._recursive_key_checker(key['elem_child'], value)
                                    if not result[0]:
                                        return (False, key['name'] + " >> " + result[1])
                            else:
                                raise UnexpectedSystemError("You-Know-who is back!!")
                    elif key['type'] == 'dict':
                        if type(config_data[key['name']]) != dict:
                            return (False, key['name'])
                        else:
                            result = self._recursive_key_checker(key['child'], config_data[key['name']])
                            if not result[0]:
                                return (False, key['name'] + " >> " + result[1])
                    else:
                        raise UnexpectedSystemError("You-Know-who is back!!")
            return (True, )
        except Exception as e:
            print("~" * 100)
            traceback.print_exc(file=sys.stdout)
            print("~" * 100)
            raise UnexpectedSystemError("Mandatory key set seems to be in wrong format, which is thouroughly unexpected. Seems like Dark Lord is back. Send an owl to Hogwarts/Burrows/Minerva/Ginny, ask for Potter.")

    def _check_for_mandatory_config(self, attr: str, config_file: str) -> typing.Tuple[bool, dict]:
        if attr == "ENVIRON_CONFIG":
            mandatory_config_keys = self.MANDATORY_ENVIRON_CONFIG_KEYS
        if attr == "REPOSITORY_CONFIG":
            mandatory_config_keys = self.MANDATORY_REPOSITORY_CONFIG_KEYS
        if attr == "CLUSTER_CONFIG":
            mandatory_config_keys = self.MANDATORY_CLUSTER_CONFIG_KEYS
        if attr == "PARENT_NODE_CONFIG":
            mandatory_config_keys = self.MANDATORY_PARENT_NODE_CONFIG_KEYS
        if attr == "CHILD_NODE_CONFIG":
            mandatory_config_keys = self.MANDATORY_CHILD_NODE_CONFIG_KEYS
        
        if pathlib.Path(config_file).suffix == ".json":
            with open(config_file, 'r') as cf:
                config_data = json.load(cf)
        else:
            with open(config_file, 'rb') as cf:
                config_data = pickle.load(cf)
        result = self._recursive_key_checker(mandatory_config_keys, config_data)
        if not result[0]:
            raise ConfigurationNotAvailableError(f"Mandatory Configuration key - " + result[1] + " is not available.")
        return (True, config_data)

    def _get_config_file(self, attr: str, config_file: str, backup_config_file: str, isinit: bool = False) -> None:
        pass

    def _get_config(self, attr: str, config_file: str, backup_config_file: str = "", isinit: bool = False) -> dict:
        config_file = self._get_config_file(attr, config_file, backup_config_file, isinit)
        if config_file:
            config_data = self._check_for_mandatory_config(attr, config_file.as_posix())
            return config_data[1]
        else:
            return {}

    @staticmethod
    def database_url_builder(env_config: dict) -> str:
        if env_config['DB_CONFIG']['DB_ENGINE'].lower() == "sqlite":
            db_file = env_config['DB_CONFIG'].get('DB_FILE', '')
            if db_file:
                database_url = 'sqlite:///{}'.format(db_file)
            else:
                raise ConfigurationNotAvailableError("Configuration key 'DB_FILE' is not available for sqlite type database.")
        else:
            raise ApplicationDatabaseUnsupportedError("Database type: {} is not supported as clustered meta-database yet. Maybe in the next version.".format(env_config['DB_CONFIG']['DB_ENGINE']))
        return database_url
