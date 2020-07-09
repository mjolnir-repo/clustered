"""
This engine will handle all environment related tasks:
 1. Initiate Clustered
 2. Refresh configurations
 3. Destroy Clustered
"""
import pathlib
import typing
import json
import pickle
import shutil
import os
import sys
import traceback
import sqlalchemy# import create_engine
import clustered.models as db_model
from clustered.engine.base_engine import ApplicationBase
from clustered.exceptions import ApplicationAlreadyInitiatedError,\
                                    ApplicationNotInitiatedError,\
                                    ApplicationWorkspaceBuildingError,\
                                    ApplicationDatabaseSetupError, \
                                    ApplicationDatabaseUnsupportedError,\
                                    ConfigurationFileNotAvailableError,\
                                    ConfigurationNotAvailableError,\
                                    ConfigurationInitiationError,\
                                    UnexpectedSystemError


class ApplicationEngine(ApplicationBase):

    @staticmethod
    def _get_config_file(attr: str, config_file: str = "", backup_config_file: str = "", isinit:bool = False) -> typing.Union[pathlib.Path, bool]:
        # Detecting configuration file
        if config_file:
            if type(config_file) == str:
                config_file = pathlib.Path(config_file)
            if not config_file.is_file():
                raise ConfigurationFileNotAvailableError(f"Provided Configuration file - '{config_file.as_posix()}' is not available.")
        elif os.environ.get("CLUSTERED__" + attr + "_FILE", ""):
            config_file = pathlib.Path(os.environ["CLUSTERED__" + attr + "_FILE"])
            if not config_file.is_file():
                raise ConfigurationFileNotAvailableError(f"Declared Configuration file - '{config_file.as_posix()}' is not available.")
        elif backup_config_file:
            backup_config_file = pathlib.Path(backup_config_file)
            if backup_config_file.is_file():
                config_file = backup_config_file
            elif attr == "ENVIRON_CONFIG":
                raise ConfigurationFileNotAvailableError(f"Backup Configuration file - '{backup_config_file.as_posix()}' is not available.")
        elif isinit and attr == "ENVIRON_CONFIG":
            raise ConfigurationFileNotAvailableError(f"Environment Configuration file must be provided while initiating the application, possible options to provide the same are \n1. use '-env_config' option while executing the command,\n2. Declare CLUSTERED__ENVIRON_CONFIG_FILE in environment,\n3. Provide '{backup_config_file}' relative to execution directory.")
        else:
            return False
        return config_file

    def _setup_wrkspc(self) -> pathlib.Path:
        wrkspc = self._get_wrkspc()
        if wrkspc.exists():
            raise ApplicationAlreadyInitiatedError()
        try:
            wrkspc.mkdir(exist_ok=False)
        except Exception as e:
            print('~' * 100)
            traceback.print_exc(file=sys.stdout)
            print('~' * 100)
            raise ApplicationWorkspaceBuildingError(f"Unexpected error occured while creating Workspace - {wrkspc}. Please check the traceback to solve the issue. You can also create the Workspace manually.")
        return wrkspc

    @staticmethod
    def _build_configuration(wrkspc: pathlib.Path, attr: str, config_data: dict) -> bool:
        try:
            config_file = wrkspc / ("." + attr + ".pkl") 
            with open(config_file.as_posix(), "wb+") as cf:
                pickle.dump(config_data, cf)
            return True
        except Exception as e:
            print('~' * 100)
            traceback.print_exc(file=sys.stdout)
            print('~' * 100)
            raise ConfigurationInitiationError(f"Failed to write the configuration file - {config_file.as_posix()} to workspace = {wrkspc}.")

    def _setup_meta_db(self) -> None:
        try:
            wrkspc = self._get_wrkspc()
            config_file = wrkspc / (".ENVIRON_CONFIG.pkl")
            if not config_file.is_file():
                raise ConfigurationFileNotAvailableError(f"{attr} type Configuration file is not available in the system. Either provide configuration file while executing the command or use methods to configure the same in the system.")
            env_config = self._get_config("ENVIRON_CONFIG", config_file)
            database_url = self.database_url_builder(env_config)
            engine = sqlalchemy.create_engine(database_url)
            db_model.Base.metadata.create_all(engine)
            return True
        except Exception as e:
            print('~' * 100)
            traceback.print_exc(file=sys.stdout)
            print('~' * 100)
            raise ApplicationDatabaseSetupError()

    # def _cleanup_meta_db(self, env_config, engine) -> None:
    #     try:
    #         database_url = self.database_url_builder(env_config)
    #         engine = create_engine(database_url)
    #         Base.metadata.drop_all(engine)
    #     except Exception as e:
    #         print('~' * 100)
    #         traceback.print_exc(file=sys.stdout)
    #         print('~' * 100)
    #         raise ApplicationDatabaseCleanupError()

    def initiate(self, **kwargs) -> None:
        try:
            wrkspc = self._setup_wrkspc()
            env_config = self._get_config("ENVIRON_CONFIG", kwargs.get('env_config_file', ''), 'config/Environment_config.json', True)
            self._build_configuration(wrkspc, "ENVIRON_CONFIG", env_config)
            repo_config = self._get_config("REPOSITORY_CONFIG", kwargs.get('repo_config_file', ''), 'config/Repository_config.json', True)
            if repo_config:
                self._build_configuration(wrkspc, "REPOSITORY_CONFIG", repo_config)
            clus_config = self._get_config("CLUSTER_CONFIG", kwargs.get('clus_config_file', ''), 'config/Cluster_config.json', True)
            if clus_config:
                self._build_configuration(wrkspc, "CLUSTER_CONFIG", clus_config)
            pnode_config = self._get_config("PARENT_NODE_CONFIG", kwargs.get('pnode_config_file', ''), 'config/Parenet_Node_config.json', True)
            if pnode_config:
                self._build_configuration(wrkspc, "PARENT_NODE_CONFIG", pnode_config)
            cnode_config = self._get_config("CHILD_NODE_CONFIG", kwargs.get('cnode_config_file', ''), 'config/Child_Node_config.json', True)
            if cnode_config:
                self._build_configuration(wrkspc, "CHILD_NODE_CONFIG", cnode_config)
            self._setup_meta_db()
            return True
        except Exception as e:
            if self.isinitiated():
                wrkspc = self._get_wrkspc()
                shutil.rmtree(wrkspc.as_posix())
            raise(e)

    def refresh(self, **kwargs) -> None:
        self.isinitiated()
        wrkspc = self._get_wrkspc()
        repo_config = self._get_config("REPOSITORY_CONFIG", kwargs.get('repo_config_file', ''), 'config/Repository_config.json')
        if repo_config:
            self._build_configuration(wrkspc, "REPOSITORY_CONFIG", repo_config)
        clus_config = self._get_config("CLUSTER_CONFIG", kwargs.get('clus_config_file', ''), 'config/Cluster_config.json')
        if clus_config:
            self._build_configuration(wrkspc, "CLUSTER_CONFIG", clus_config)
        pnode_config = self._get_config("PARENT_NODE_CONFIG", kwargs.get('pnode_config_file', ''), 'config/Parenet_Node_config.json')
        if pnode_config:
            self._build_configuration(wrkspc, "PARENT_NODE_CONFIG", pnode_config)
        cnode_config = self._get_config("CHILD_NODE_CONFIG", kwargs.get('cnode_config_file', ''), 'config/Child_Node_config.json')
        if cnode_config:
            self._build_configuration(wrkspc, "CHILD_NODE_CONFIG", cnode_config)
        return True

    def destroy(self) -> None:
        try:
            self.isinitiated()
            wrkspc = self._get_wrkspc()
            config_file = wrkspc / (".ENVIRON_CONFIG.pkl")
            if not config_file.is_file():
                raise ConfigurationFileNotAvailableError(f"Environment Configuration file {config_file.as_posix()} is not available in the system. Either provide configuration file while executing the command or use methods to configure the same in the system.")
            env_config = self._get_config("ENVIRON_CONFIG", config_file)
            if env_config['DB_CONFIG']['DB_ENGINE'].lower() == 'sqlite':
                pathlib.Path(env_config['DB_CONFIG']['DB_FILE']).unlink()
                shutil.rmtree(wrkspc.as_posix())
            return True
        except Exception as e:
            print('~' * 100)
            traceback.print_exc(file=sys.stdout)
            print('~' * 100)
            raise(e)
