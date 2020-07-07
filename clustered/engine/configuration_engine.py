"""
Declare environmetal variables like database details, user details etc
"""
import os
import sys
import json
import typing
import pathlib
from clustered.exceptions import ConfigurationNotAvailableError, ConfigurationFileNotAvailableError
from clustered.engine.base_engine import ApplicationBase
import traceback


class ApplicationConfiguration(ApplicationBase):

    @staticmethod
    def _get_config_file(attr: str, config_file: str) -> typing.Union[pathlib.Path, bool]:
        # Detecting configuration file
        if config_file:
            if type(config_file) == str:
                config_file = pathlib.Path(config_file)
            if not config_file.is_file():
                raise ConfigurationFileNotAvailableError(f"Provided Configuration file - '{config_file.as_posix()}' is not available.")
        else:
            wrkspc = self._get_wrkspc()
            config_file = wrkspc / ("." + attr + ".pkl")
            if not config_file.is_file():
                raise ConfigurationFileNotAvailableError(f"{attr} type Configuration file is not available in the system. Either provide configuration file while executing the command or use methods to configure the same in the system.")
        return config_file


class EnvironmentConfiguration(ApplicationConfiguration):
    CACHED_OBJ = None

    def __new__(cls):
        if not cls.CACHED_OBJ:
            return super(EnvironmentConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self):
        self.isinitiated()
        self.config_data = self._get_config("ENVIRON_CONFIG", "")
        self.CACHED_OBJ = self

    def __call__(self):
        return self.config_data


class RepositoryConfiguration(ApplicationConfiguration):
    CACHED_OBJ = None

    def __new__(cls, config_file: str = ''):
        if config_file or (not cls.CACHED_OBJ):
            return super(RepositoryConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ

    def __init__(self, config_file: str = ''):
        self.isinitiated()
        self.config_data = self._get_config("REPOSITORY_CONFIG", config_file)
        self.CACHED_OBJ = self

    def __call__(self):
        return self.config_data


class ClusterConfiguration(ApplicationConfiguration):
    CACHED_OBJ = None

    def __new__(cls, config_file: str = ''):
        if config_file or (not cls.CACHED_OBJ):
            return super(ClusterConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self, config_file: str = ''):
        self.isinitiated()
        self.config_data = self._get_config("CLUSTER_CONFIG", config_file)
        self.CACHED_OBJ = self

    def __call__(self):
        return self.config_data


class ParentNodeConfiguration(ApplicationConfiguration):
    CACHED_OBJ = None

    def __new__(cls, config_file: str = ''):
        if config_file or (not cls.CACHED_OBJ):
            return super(ParentNodeConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self, config_file: str = ''):
        self.isinitiated()
        self.config_data = self._get_config("PARENT_NODE_CONFIG", config_file)
        self.CACHED_OBJ = self

    def __call__(self):
        return self.config_data


class ChildNodeConfiguration(ApplicationConfiguration):
    CACHED_OBJ = None

    def __new__(cls, config_file: str = ''):
        if config_file or (not cls.CACHED_OBJ):
            return super(ChildNodeConfiguration, cls).__new__(cls)
        else:
            return cls.CACHED_OBJ
    
    def __init__(self, config_file: str = ''):
        self.isinitiated()
        self.config_data = self._get_config("CHILD_NODE_CONFIG", config_file)
        self.CACHED_OBJ = self

    def __call__(self):
        return self.config_data


class Engine:

    def env(self) -> EnvironmentConfiguration:
        return EnvironmentConfiguration()
    
    def repo(self, config_file:str='') -> RepositoryConfiguration:
        return RepositoryConfiguration(config_file)
    
    def cluster(self, config_file:str='') -> ClusterConfiguration:
        return ClusterConfiguration(config_file)
    
    def parent(self, config_file:str='') -> ParentNodeConfiguration:
        return ParentNodeConfiguration(config_file)

    def child(self, config_file:str='') -> ChildNodeConfiguration:
        return ChildNodeConfiguration(config_file)


engine = Engine()
