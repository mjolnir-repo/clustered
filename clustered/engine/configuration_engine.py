"""
Declare environmetal variables like database details, user details etc
"""
import os
import sys
import json
import pathlib
from clustered.exceptions import ConfigurationNotAvailableError, ConfigurationFileNotAvailableError
import traceback



class Engine:

    def env(self, config_file=''):
        return EnvironmentConfiguration(config_file)()
    
    def repo(self, config_file=''):
        return RepositoryConfiguration(config_file)()
    
    def cluster(self, config_file=''):
        return ClusterConfiguration(config_file)()
    
    def parent(self, config_file=''):
        return ParentNodeConfiguration(config_file)()

    def child(self, config_file=''):
        return ChildNodeConfiguration(config_file)()


class Configuration:

    def _get_config(self, attr, config_file, backup_config_file):
        config = {}
        
        # Detecting configuration file
        if config_file:
            if not os.path.isfile(config_file):
                raise ConfigurationFileNotAvailableError(f"Provided Configuration file - '{config_file}' is not available.")
        elif os.environ.get("CLUSTERED__" + attr, ""):
            config_file = os.environ["CLUSTERED__" + attr]
        elif os.path.isfile(backup_config_file):
            config_file = backup_config_file
        else:
            raise ConfigurationFileNotAvailableError(f"Configuration file name is not provided, possible options to provide the same are \n1. use '-config' option while executing the command,\n2. Declare CLUSTERED__{attr} in environment,\n3. Provide {backup_config_file} relative to execution directory.")
        
        # Reading detected configuration file
        try:
            # TODO: Incorporate `dattr`
            with open(config_file, 'r') as f:
                config = json.load(f)
        except:
            print('~' * 75)
            traceback.print_exc(file=sys.stdout)
            print('~' * 75)
            raise ConfigurationExtractionError()

        # Checking for all mandatory configuration keys
        for key in self.MANDATORY_CONFIG_KEY:
            if config.get(key, ""):
                pass
            elif os.environ.get("CLUSTERED__" + key, ""):
                config[key] = os.environ["CLUSTERED__" + key]
            else:
                raise ConfigurationNotAvailableError(f"Mandatory Configuration key - '{key}' is not available. User can provide the same in the configuration file or it can be declared in environment using 'CLUSTERED__{key}' key.")

        return (config_file, config)


class EnvironmentConfiguration(Configuration):
    MANDATORY_CONFIG_KEY = ["DB_ENGINE", "DB_FILE"]
    def __init__(self, config_file = ''):
        self.config_file, self.configuration = self._get_config("ENVIRON_CONFIG_FILE", config_file, 'config/Environment_config.json')

        # Building Database URL
        if self.configuration['DB_ENGINE'].lower() == "sqlite":
            self.configuration['DATABASE_URL'] = 'sqlite:///{}'.format(self.configuration['DB_FILE'])

    def __call__(self):
        return self.configuration


class RepositoryConfiguration(Configuration):
    MANDATORY_CONFIG_KEY = ["CLOUD_TYPE"]
    def __init__(self, config_file = ''):
        self.config_file, self.configuration = self._get_config("REPOSITORY_CONFIG_FILE", config_file, 'config/Repository_config.json')

    def __call__(self):
        return self.configuration


class ClusterConfiguration(Configuration):
    def __init__(self, config_file = ''):
        self.MANDATORY_CONFIG_KEY = []
        self.config_file, self.configuration = self._get_config("CLUSTER_CONFIG_FILE", config_file, 'config/Cluster_config.json')

    def __call__(self):
        return self.configuration


class ParentNodeConfiguration(Configuration):
    def __init__(self, config_file = ''):
        self.MANDATORY_CONFIG_KEY = []
        self.config_file, self.configuration = self._get_config("PARENT_NODE_CONFIG_FILE", config_file, 'config/Parent_Node_config.json')

    def __call__(self):
        return self.configuration


class ChildNodeConfiguration(Configuration):
    def __init__(self, config_file = ''):
        self.MANDATORY_CONFIG_KEY = []
        self.config_file, self.configuration = self._get_config("CHILD_NODE_CONFIG_FILE", config_file, 'config/Child_Node_config.json')

    def __call__(self):
        return self.configuration


# class EnvironmetConfiguration:
#     def __init__(self):
#         pass
    
#     @classmethod
#     def _get_env_config(cls):
#         app_path = os.environ.get('CLUSTERED_APP_PATH')
#         env_file = app_path + '/clustered/config/Environment_Config.json'
#         with open(env_file, 'r') as env_file_content:
#             env_file_data = json.load(env_file_content)
#         return (app_path, env_file, env_file_data)


# class Environment:
# 	def __init__(self):
# 		self.APP_PATH, self.ENV_FILE, self.ENV_CONFIG = EnvironmetConfiguration._get_env_config()
# 		self.AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
# 		self.AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
# 		self.AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION', '')
# 		self.CLUSTER_CONFIG_FILE = self.APP_PATH + '/clustered/config/Cluster_Config.json'
# 		self.MASTER_NODE_CONFIG_FILE = self.APP_PATH + '/clustered/config/Master_Node_Config.json'
# 		self.SLAVE_NODE_CONFIG_FILE = self.APP_PATH + '/clustered/config/Slave_Node_Config.json'

# 	@property
# 	def DATABASE_URL(self):
# 		_db_list = self.ENV_CONFIG.get('DB', [])
# 		if _db_list:
# 			_active_db = {}
# 			for _db in _db_list:
# 				if _db.get('DB_ACTIVE_FLAG', 0):
# 					_active_db = _db
# 					break
# 			else:
# 				raise Exception("No Active database configuration is available in " + self.ENV_FILE)
# 		else:
# 			raise Exception("Database configuratoin is not present in " + self.ENV_FILE + " Please add the same or refer documentation.")
# 		if _active_db.get('DB_ENGINE', 'sqlite').lower() == 'sqlite':
# 			_db_file = _active_db.get('DB_DETAILS', {}).get('DB_FILE','clustered.db')
# 			return f'sqlite:///{_db_file}'

engine = Engine()
