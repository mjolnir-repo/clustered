"""
All repository related tasks will be defined here.
These tasks will be independent of any object which are not present in this module. 
"""
from clustered.models import Repository
from clustered.engine.database_engine import DatabaseEngine
from clustered.engine.configuration_engine import engine as conf_engine


class RepositoryEngine:
    def __init__(self, env_config_file:str = ""):
        self.db_engine = DatabaseEngine(env_config_file)

    def add_repository(self, repo_name:str, enc_name:str, aws_access_key:str, aws_secret_key:str, aws_region:str, repo_config_filepath:str = '') -> None:
        repo_config = conf_engine.repo(repo_config_filepath)
        if not aws_access_key:
            aws_access_key = repo_config.AWS_ACCESS_KEY_ID
        if not aws_secret_key:
            aws_secret_key = repo_config.AWS_SECRET_ACCESS_KEY
        if not aws_region:
            aws_region = repo_config.AWS_DEFAULT_REGION
        #TO DO: Write all AWS related Code here.
        return self.db_engine.add_repository(repo_name, enc_name, aws_access_key, aws_secret_key, aws_region)

    def describe_repository(self, repo_name:str, check_active_flag:bool = False) -> Repository:
        return self.db_engine.get_repository_by_name(repo_name.upper(), check_active_flag)

    def list_repositories(self, check_active_flag:bool = False) -> [Repository]:
        if check_active_flag:
            all_repositories = self.db_engine.get_active_repositories()
        else:
            all_repositories = self.db_engine.get_all_repositories()
        return [
                {
                    'REPO_NAME': repo.REPO_NAME,
                    'REPO_ACTIVE_FLAG': repo.REPO_ACTIVE_FLAG
                }
                for repo in all_repositories
            ]

    def delete_repository(self, repo_name:str, hard:bool = False) -> None:
        #TO DO: Write all AWS related Code here.
        self.db_engine.delete_repository_by_name(repo_name, hard)

    def recover_repository(self, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        self.db_engine.recover_repository_by_name(repo_name)

    def flush_repositories(self) -> None:
        #TO DO: Write all AWS related Code here.
        self.db_engine.clean_up_inactive_repositories()

    def purge_all_repositories(self) -> None:
        #TO DO: Write all AWS related Code here.
        self.db_engine.clean_up_all_repositories()
