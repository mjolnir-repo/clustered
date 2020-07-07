import sys
import sqlalchemy
from sqlalchemy import create_engine, exc, and_
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from clustered.models import Encryptor, Repository
from clustered.encrypt import Encrypt
from clustered.exceptions import EncryptorNotPresentError, EncryptorNotActiveError, EncryptorAlreadyExistsError, WrongActionInvocationError, RepositoryNotPresentError, RepositoryNotActiveError, RepositoryAlreadyExistsError, EncryptorNotPresentError
from clustered.engine.configuration_engine import engine as conf_engine


class DatabaseEngine:
    def __init__(self):
        env_obj = conf_engine.env()
        env_config = env_obj()
        self.engine = create_engine(env_obj.database_url_builder(env_config))

    # DB session builder
    @contextmanager
    def session_scope(self):
        _Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        session  = _Session()
        try:
            yield session
        except Exception as e:
            # print("ERROR: Exception occured while database session was open.")
            # print(str(e))
            session.rollback()
            raise e
        else:
            # print("INFO: Database operations completed successfully.")
            session.commit()
        finally:
            # print("DEBUG: Expunging objects from session.")
            # session.expunge_all()
            session.close()

    # Adhoc query handler
    def adhoc_select_query(sejf):
        pass
    
    def adhoc_non_select_query(self):
        pass

    # Encryptor related DB activity
    def add_encryptor(self, enc_name:str) -> Encryptor:
        try:
            with self.session_scope() as session:
                enc_obj = Encryptor(
                    ENC_NAME=enc_name.upper(),
                    ENC_KEY = Encrypt.get_hashed_key(),
                    ENC_ACTIVE_FLAG = 'Y'
                )
                session.add(enc_obj)
            return self.get_encryptor_by_name(enc_name)
        except exc.IntegrityError as e:
            raise EncryptorAlreadyExistsError()

    def get_encryptor_by_name(self, enc_name:str, active_check_flag:bool = False) -> Encryptor:
        with self.session_scope() as sess:
            encryptor = sess.query(Encryptor)\
                .filter(
                    Encryptor.ENC_NAME == enc_name.upper()
                )\
                .first()
        if encryptor:
            if active_check_flag and encryptor.ENC_ACTIVE_FLAG != 'Y':
                raise EncryptorNotActiveError(f"Encryptor<'{enc_name}'> is not in ACTIVE state. Recover the encryptor before using it, or use another one.")
            else:
                return encryptor
        else:
            raise EncryptorNotPresentError()

    def get_all_encryptors(self) -> [Encryptor]:
        with self.session_scope() as sess:
            encryptors = sess.query(Encryptor)\
                .all()
        if encryptors:
            return encryptors
        else:
            raise EncryptorNotPresentError()

    def get_active_encryptors(self) -> [Encryptor]:
        with self.session_scope() as sess:
            encryptor = sess.query(Encryptor)\
                .filter(
                    Encryptor.ENC_ACTIVE_FLAG == 'Y'
                )\
                .all()
        if encryptor:
            return encryptor
        else:
            raise EncryptorNotPresentError()

    def delete_encryptor_by_name(self, enc_name:str, hard:bool = False) -> None:
        with self.session_scope() as sess:
            enc_obj = self.get_encryptor_by_name(enc_name)
            if hard:
                sess.delete(enc_obj)
            else:
                enc_obj.ENC_ACTIVE_FLAG = 'N'
                sess.add(enc_obj)

    def recover_encryptor_by_name(self, enc_name:str) -> None:
        with self.session_scope() as sess:
            enc_obj = self.get_encryptor_by_name(enc_name)
            if enc_obj.ENC_ACTIVE_FLAG == 'Y':
                raise WrongActionInvocationError(f"Encryptor<'{enc_name}'> is already Active.")
            else:
                enc_obj.ENC_ACTIVE_FLAG = 'Y'
                sess.add(enc_obj)

    def clean_up_inactive_encryptors(self) -> None:
        with self.session_scope() as session:
            session.query(Encryptor)\
                .filter(Encryptor.ENC_ACTIVE_FLAG == 'N')\
                .delete(synchronize_session=False)

    def clean_up_all_encryptors(self) -> None:
        with self.session_scope() as session:
            session.query(Encryptor)\
                .delete(synchronize_session=False)


    # Repository related DB activity
    def add_repository(self, repo_name:str, enc_name:str, aws_access_key:str, aws_secret_key:str, aws_region:str) -> None:
        try:
            enc_obj = self.get_encryptor_by_name(enc_name, True)
            #TO DO: Write all AWS related Code here.
            with self.session_scope() as session:
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
            return self.get_repository_by_name(repo_name)
        except exc.IntegrityError as e:
            raise RepositoryAlreadyExistsError()

    def get_repository_by_name(self, repo_name:str, active_check_flag:bool = False) -> Repository:
        with self.session_scope() as sess:
            repository = sess.query(Repository)\
                .filter(
                    Repository.REPO_NAME == repo_name.upper()
                )\
                .first()
            if not repository:
                raise RepositoryNotPresentError()
            elif active_check_flag and repository.REPO_ACTIVE_FLAG != 'Y':
                raise RepositoryNotActiveError(f"Repository<'{repo_name}'> is not in ACTIVE state. Recover the repostory before using it, or use another one.")
            return repository

    def get_all_repositories(self) -> [Repository]:
        with self.session_scope() as sess:
            repositories = sess.query(Repository)\
                .all()
        if repositories:
            return repositories
        else:
            raise RepositoryNotPresentError()

    def get_active_repositories(self) -> [Repository]:
        with self.session_scope() as sess:
            repository = sess.query(Repository)\
                .filter(
                    Repository.REPO_ACTIVE_FLAG == 'Y'
                )\
                .all()
        if repository:
            return repository
        else:
            raise RepositoryNotPresentError()

    def delete_repository_by_name(self, repo_name:str, hard:bool = False) -> None:
        with self.session_scope() as sess:
            repo_obj = self.get_repository_by_name(repo_name)
            if hard:
                sess.delete(repo_obj)
            else:
                repo_obj.REPO_ACTIVE_FLAG = 'N'
                sess.add(repo_obj)

    def recover_repository_by_name(self, repo_name:str) -> None:
        with self.session_scope() as sess:
            repo_obj = self.get_repository_by_name(repo_name)
            if repo_obj.REPO_ACTIVE_FLAG == 'Y':
                raise WrongActionInvocationError(f"Repository<'{repo_name}'> is already Active.")
            else:
                repo_obj.REPO_ACTIVE_FLAG = 'Y'
                sess.add(repo_obj)

    def clean_up_inactive_repositories(self) -> None:
        with self.session_scope() as session:
            session.query(Repository)\
                .filter(Repository.REPO_ACTIVE_FLAG == 'N')\
                .delete(synchronize_session=False)

    def clean_up_all_repositories(self) -> None:
        with self.session_scope() as session:
            session.query(Repository)\
                .delete(synchronize_session=False)

# db_engine = DatabaseEngine()