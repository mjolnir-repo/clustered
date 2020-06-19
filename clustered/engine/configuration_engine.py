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
    config_set = {}

    def env(self, config_file:str='', refresh:bool=False):
        if refresh or config_file or (not Engine.config_set.get('ENV_CONFIG', None)):
            Engine.config_set['ENV_CONFIG'] = env_config = EnvironmentConfiguration(config_file, refresh=refresh)()
            return env_config
        else:
            return Engine.config_set['ENV_CONFIG']
    
    def repo(self, config_file:str='', refresh:bool=False):
        if refresh or config_file or (not Engine.config_set.get('REPO_CONFIG', None)):
            Engine.config_set['REPO_CONFIG'] = repo_config = RepositoryConfiguration(config_file, refresh=refresh)()
            return repo_config
        else:
            return Engine.config_set['REPO_CONFIG']
    
    def cluster(self, config_file:str='', refresh:bool=False):
        if refresh or config_file or (not Engine.config_set.get('CLUSTER_CONFIG', None)):
            Engine.config_set['CLUSTER_CONFIG'] = cluster_config = ClusterConfiguration(config_file, refresh=refresh)()
            return cluster_config
        else:
            return Engine.config_set['CLUSTER_CONFIG']
    
    def parent(self, config_file:str='', refresh:bool=False):
        if refresh or config_file or (not Engine.config_set.get('PARENT_NODE_CONFIG', None)):
            Engine.config_set['PARENT_NODE_CONFIG'] = parent_node_config = ParentNodeConfiguration(config_file, refresh=refresh)()
            return parent_node_config
        else:
            return Engine.config_set['PARENT_NODE_CONFIG']

    def child(self, config_file:str='', refresh:bool=False):
        if refresh or config_file or (not Engine.config_set.get('CHILD_NODE_CONFIG', None)):
            Engine.config_set['CHILD_NODE_CONFIG'] = child_node_config = ChildNodeConfiguration(config_file, refresh=refresh)()
            return child_node_config
        else:
            return Engine.config_set['CHILD_NODE_CONFIG']


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
    CACHED_OBJ = None

    def __new__(cls, config_file:str = '', refresh:bool = False):
        if refresh or config_file or (not cls.CACHED_OBJ):
            return super(EnvironmentConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self, config_file:str = '', refresh:bool = False):
        self.config_file, self.configuration = self._get_config("ENVIRON_CONFIG_FILE", config_file, 'config/Environment_config.json')
        # Building Database URL
        if self.configuration['DB_ENGINE'].lower() == "sqlite":
            self.configuration['DATABASE_URL'] = 'sqlite:///{}'.format(self.configuration['DB_FILE'])
        self.CACHED_OBJ = self

    def __call__(self):
        return self.configuration


class RepositoryConfiguration(Configuration):
    MANDATORY_CONFIG_KEY = ["CLOUD_TYPE"]
    CACHED_OBJ = None

    def __new__(cls, config_file:str = '', refresh:bool = False):
        if refresh or config_file or (not cls.CACHED_OBJ):
            return super(RepositoryConfiguration, cls).__new__(cls)
        else:
            print('Using old object')
            print(cls.CACHED_OBJ)
            return cls.CACHED_OBJ

    def __init__(self, config_file = '', refresh:bool = False):
        self.config_file, self.configuration = self._get_config("REPOSITORY_CONFIG_FILE", config_file, 'config/Repository_config.json')
        RepositoryConfiguration.CACHED_OBJ = self

    def __call__(self):
        return self.configuration


class ClusterConfiguration(Configuration):
    MANDATORY_CONFIG_KEY = []
    CACHED_OBJ = None

    def __new__(cls, config_file:str = '', refresh:bool = False):
        if refresh or config_file or (not cls.CACHED_OBJ):
            return super(ClusterConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self, config_file = '', refresh:bool = False):
        self.config_file, self.configuration = self._get_config("CLUSTER_CONFIG_FILE", config_file, 'config/Cluster_config.json')
        self.CACHED_OBJ = self

    def __call__(self):
        return self.configuration


class ParentNodeConfiguration(Configuration):
    MANDATORY_CONFIG_KEY = []
    CACHED_OBJ = None

    def __new__(cls, config_file:str = '', refresh:bool = False):
        if refresh or config_file or (not cls.CACHED_OBJ):
            return super(ParentNodeConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self, config_file = '', refresh:bool = False):
        self.config_file, self.configuration = self._get_config("PARENT_NODE_CONFIG_FILE", config_file, 'config/Parent_Node_config.json')
        self.CACHED_OBJ = self

    def __call__(self):
        return self.configuration


class ChildNodeConfiguration(Configuration):
    MANDATORY_CONFIG_KEY = []
    CACHED_OBJ = None

    def __new__(cls, config_file:str = '', refresh:bool = False):
        if refresh or config_file or (not cls.CACHED_OBJ):
            return super(ChildNodeConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self, config_file = '', refresh:bool = False):
        self.config_file, self.configuration = self._get_config("CHILD_NODE_CONFIG_FILE", config_file, 'config/Child_Node_config.json')
        self.CACHED_OBJ = self

    def __call__(self):
        return self.configuration

engine = Engine()
