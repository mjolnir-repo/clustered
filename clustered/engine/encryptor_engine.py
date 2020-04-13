"""
Encryptor related tasks will be abstracted from here.
Some example tasks:
 1. Create new encryptor
 2. Extract existing encryptor
 3. Delete existing encryptor
"""

import pickle
from cryptography.fernet import Fernet
from ..database import db_obj
from ..models import Encryptor
from sqlalchemy import and_
from ..exceptions import EncryptorNotPresentError, EncryptorAlreadyExistsError
from sqlalchemy import exc
import sys


class Encrypt:
    def __init__(self):
        pass

    @staticmethod
    def get_hashed_key(__key):
        return pickle.dumps(__key)

    @staticmethod
    def __unhash(__hashed_key):
        return pickle.loads(__hashed_key)

    @classmethod
    def __get_encryptor_suite(cls, __hashed_key):
        __key = cls.__unhash(__hashed_key)
        return Fernet(__key)

    @classmethod
    def encrypt(cls, __plain_text, __hashed_key):
        __encryptor = cls.__get_encryptor_suite(__hashed_key)
        __hashed_text = cls.get_hashed_key(__plain_text)
        return __encryptor.encrypt(__hashed_text)


class EncryptorEngine:
    def __init__(self):
        pass

    @classmethod
    def get_encryptor_by_name(cls, enc_name:str) -> Encryptor:
        enc_name = enc_name.upper() or 'DEFAULT_ENC'
        with db_obj.session_scope() as sess:
            encryptor = [enc for enc in sess.query(Encryptor).filter(and_(Encryptor.ENC_ACTIVE_FLAG == 'Y', Encryptor.ENC_NAME == enc_name.upper()))]
        if encryptor:
            return encryptor[0]
        else:
            raise EncryptorNotPresentError()

    @classmethod
    def list_encryptors(cls) -> [Encryptor]:
        with db_obj.session_scope() as sess:
            encryptors = [enc for enc in  sess.query(Encryptor).filter(Encryptor.ENC_ACTIVE_FLAG == 'Y')]
        return encryptors

    @classmethod
    def add_encryptor(cls, enc_name:str) -> bool:
        try:
            __key = Fernet.generate_key()
            with db_obj.session_scope() as session:
                enc_obj = Encryptor(
                    ENC_NAME=enc_name.upper(),
                    ENC_KEY = Encrypt.get_hashed_key(__key),
                    ENC_ACTIVE_FLAG = 'Y'
                )
                session.add(enc_obj)
            return True
        except exc.IntegrityError as e:
            raise EncryptorAlreadyExistsError()
        except Exception as e:
            print("ERROR: Exception occured while creating repository: " + str(e))
            return False
        
    
    @classmethod
    def delete_encryptor(cls, enc_name:str) -> bool:
        try:
            with db_obj.session_scope() as session:
                session.query(Encryptor).filter(and_(Encryptor.ENC_NAME == enc_name.upper(), Encryptor.ENC_ACTIVE_FLAG == 'Y')).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while creating repository: " + str(e))
            return False


    @classmethod
    def purge_all_encryptors(cls) -> bool:
        try:
            with db_obj.session_scope() as session:
                session.query(Encryptor).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while creating repository: " + str(e))
            return False