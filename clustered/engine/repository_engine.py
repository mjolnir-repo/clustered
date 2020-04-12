"""
All repository related tasks will be defined here.
These tasks will be independent of any object which are not present in this module. 
"""
import pprint
from ..models import Repository, Cluster, Node
from ..database import db_obj
from ..env import env


class RepositoryEngine:
    def __init__(self):
        pass

    @classmethod
    def list_repositories(cls) -> [Repository]:
        with db_obj.session_scope() as session:
            repo_list = [repo for repo in session.query(Repository).filter(Repository.REPO_ACTIVE_FLAG == 'Y')]
            # for repo in session.query(Repository).filter(Repository.REPO_ACTIVE_FLAG == 'Y'):
            #     pprint.pprint(repo)
        return repo_list

    @classmethod
    def create_repository(cls, repo_name:str, aws_access_key:str, aws_secret_key:str, aws_region:str) -> bool:
        try:
            if not aws_access_key:
                aws_access_key = env.AWS_ACCESS_KEY_ID
            if not aws_secret_key:
                aws_secret_key = env.AWS_SECRET_ACCESS_KEY
            if not aws_region:
                aws_region = env.AWS_DEFAULT_REGION

            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                repo_obj = Repository(
                    REPO_NAME=repo_name.upper(),
                    #TO DO: Encrypt the keys
                    REPO_ACCESS_KEY_ENCRYPTED=aws_access_key,
                    REPO_SECRET_KEY_ENCRYPTED=aws_secret_key,
                    REPO_REGION=aws_region,
                    REPO_STATE='AVAILABLE',
                    REPO_ACTIVE_FLAG='Y'
                )
                session.add(repo_obj)
            return True
        except Exception as e:
            print("ERROR: Exception occured while creating repository: " + str(e))
            return False

    @classmethod
    def delete_repository(cls, repo_name:str) -> bool:
        try:
            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                session.query(Repository).filter(Repository.REPO_NAME == repo_name.upper()).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while deleting repository: " + str(e))
            return False

    @classmethod
    def delete_repository_all(cls) -> bool:
        try:
            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                session.query(Repository).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while deleting repository: " + str(e))
            return False
