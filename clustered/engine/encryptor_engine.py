"""
Encryption related tasks will be abstracted from here. This module serves both 
Encrypt:

 1. Create new encryptor
 2. Extract existing encryptor
 3. Delete existing encryptor
"""

from clustered.models import Encryptor
from clustered.engine.database_enfgine import db_engine
import sys


class EncryptorEngine:
    def __init__(self):
        pass

    @staticmethod
    def add_encryptor(enc_name:str) -> None:
        db_engine.add_encryptor(enc_name)

    @staticmethod
    def describe_encryptor(enc_name:str) -> Encryptor:
        enc_name = enc_name.upper() or 'DEFAULT_ENC'
        return db_engine.get_encryptor_by_name(enc_name)
 
    @staticmethod
    def list_encryptors() -> [Encryptor]:
        all_encryptors = db_engine.get_all_encryptors()
         return 
            [
                {
                    'ENC_NAME': enc.ENC_NAME,
                    'ENC_ACTIVE_FLAG': enc.ENC_ACTIVE_FLAG
                }
                for enc in all_encryptors
            ]
    
    @staticmethod
    def delete_encryptor(enc_name:str) -> None:
        db_engine.delete_encryptor_by_name(enc_name)

    @staticmethod
    def recover_encryptor(enc_name:str) -> None:
        db_engine.recover_encryptor_by_name(enc_name)

    @staticmethod
    def flush_encryptors() -> None:
        db_engine.clean_up_inactive_encryptors()

    @staticmethod
    def purge_all_encryptors() -> None:
        db_engine.clean_up_all_encryptors()