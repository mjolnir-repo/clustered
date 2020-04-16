"""
Encryption related tasks will be abstracted from here. This module serves both 
Encrypt:

 1. Create new encryptor
 2. Extract existing encryptor
 3. Delete existing encryptor
"""

from ..database import db_obj
from ..models import Encryptor
from sqlalchemy import and_
from ..exceptions import EncryptorNotPresentError, EncryptorAlreadyExistsError, WrongActionInvocationError
from ..encrypt import Encrypt
from sqlalchemy import exc
import sys


class EncryptorEngine:
    def __init__(self):
        pass


    @staticmethod
    def add_encryptor(enc_name:str) -> None:
        try:
            with db_obj.session_scope() as session:
                enc_obj = Encryptor(
                    ENC_NAME=enc_name.upper(),
                    ENC_KEY = Encrypt.get_hashed_key(),
                    ENC_ACTIVE_FLAG = 'Y'
                )
                session.add(enc_obj)
        except exc.IntegrityError as e:
            raise EncryptorAlreadyExistsError()


    @staticmethod
    def describe_encryptor(enc_name:str) -> Encryptor:
        enc_name = enc_name.upper() or 'DEFAULT_ENC'
        with db_obj.session_scope() as sess:
            encryptor = sess.query(Encryptor)\
                .filter(
                    Encryptor.ENC_NAME == enc_name.upper()
                )\
                .first()
        if encryptor:
            return encryptor
        else:
            raise EncryptorNotPresentError()

 
    @staticmethod
    def list_encryptors() -> [Encryptor]:
        with db_obj.session_scope() as sess:
            encryptors = [
                {
                    'ENC_NAME': enc.ENC_NAME,
                    'ENC_ACTIVE_FLAG': enc.ENC_ACTIVE_FLAG
                } for enc in sess.query(Encryptor)]
        return encryptors

    
    @staticmethod
    def delete_encryptor(enc_name:str) -> None:
        with db_obj.session_scope() as session:
            enc_obj = session.query(Encryptor)\
                .filter(
                    and_(
                        Encryptor.ENC_NAME == enc_name.upper()
                        , Encryptor.ENC_ACTIVE_FLAG == 'Y'
                    )
                ).first()
            enc_obj.ENC_ACTIVE_FLAG = 'N'
            session.commit()

    
    @staticmethod
    def recover_encryptor(enc_name:str) -> None:
        with db_obj.session_scope() as session:
            enc_obj = session.query(Encryptor)\
                .filter(
                    Encryptor.ENC_NAME == enc_name.upper()
                ).first()
            if enc_obj.ENC_ACTIVE_FLAG == 'Y':
                raise WrongActionInvocationError(f"Encryptor<'{enc_name}'> is already Active.")
            enc_obj.ENC_ACTIVE_FLAG = 'Y'
            session.commit()


    @staticmethod
    def flush_encryptors() -> None:
        with db_obj.session_scope() as session:
            session.query(Encryptor)\
                .filter(Encryptor.ENC_ACTIVE_FLAG == 'N')\
                .delete(synchronize_session=False)


    @staticmethod
    def purge_all_encryptors() -> None:
        with db_obj.session_scope() as session:
            session.query(Encryptor).delete(synchronize_session=False)