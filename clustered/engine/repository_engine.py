"""
All repository related tasks will be defined here.
These tasks will be independent of any object which are not present in this module. 
"""
import pprint
import pickle
from ..models import Repository, Encryptor, Cluster, Node
from ..database import db_obj
from ..env import env
from ..exceptions import RepositoryNotPresentError, RepositoryAlreadyExistsError, EncryptorNotPresentError
from ..encrypt import Encrypt
from sqlalchemy import and_, exc



class RepositoryEngine:
    def __init__(self):
        pass


    @staticmethod
    def describe_repository(repo_name:str = '') -> Repository:
        with db_obj.session_scope() as sess:
            repository = [repo for repo in sess.query(Repository).filter(and_(Repository.REPO_ACTIVE_FLAG == 'Y', Repository.REPO_NAME == repo_name.upper()))]
            if repository:
                return repository[0]
            else:
                raise RepositoryNotPresentError()


    @staticmethod
    def list_repositories() -> [Repository]:
        with db_obj.session_scope() as session:
            repo_list = [repo for repo in session.query(Repository).filter(Repository.REPO_ACTIVE_FLAG == 'Y')]
        return repo_list


    @staticmethod
    def create_repository(repo_name:str, enc_name:str, aws_access_key:str, aws_secret_key:str, aws_region:str) -> bool:
        try:
            with db_obj.session_scope() as sess:
                encryptor = [enc for enc in sess.query(Encryptor).filter(and_(Encryptor.ENC_ACTIVE_FLAG == 'Y', Encryptor.ENC_NAME == enc_name.upper()))]
                if encryptor:
                    enc_obj = encryptor[0]
                else:
                    raise EncryptorNotPresentError()

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
                    REPO_ACCESS_KEY_ENCRYPTED=Encrypt.encrypt(aws_access_key, enc_obj.ENC_KEY),
                    REPO_SECRET_KEY_ENCRYPTED=Encrypt.encrypt(aws_secret_key, enc_obj.ENC_KEY),
                    REPO_REGION=aws_region,
                    REPO_STATE='AVAILABLE',
                    REPO_ACTIVE_FLAG='Y',
                    ENCRYPTOR = enc_obj
                )
                session.add(repo_obj)
            return True
        except exc.IntegrityError as e:
            raise RepositoryAlreadyExistsError()
        except Exception as e:
            print("ERROR: Exception occured while creating repository: " + str(e))
            return False


    @staticmethod
    def delete_repository(repo_name:str) -> bool:
        try:
            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                session.query(Repository).filter(and_(Repository.REPO_NAME == repo_name.upper(), Repository.REPO_ACTIVE_FLAG == 'Y')).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while deleting repository: " + str(e))
            return False


    @staticmethod
    def purge_all_repositories() -> bool:
        try:
            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                session.query(Repository).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while deleting repository: " + str(e))
            return False
