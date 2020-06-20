"""
Encryption related tasks will be abstracted from here. This module serves both 
Encrypt:

 1. Create new encryptor
 2. Extract existing encryptor
 3. Delete existing encryptor
"""

from clustered.models import Encryptor
from clustered.engine.database_engine import DatabaseEngine
import sys


class EncryptorEngine:
    def __init__(self, env_config_file:str = ""):
        self.db_engine = DatabaseEngine(env_config_file)

    def add_encryptor(self, enc_name:str) -> None:
        encryptor = self.db_engine.add_encryptor(enc_name)
        return encryptor

    def describe_encryptor(self, enc_name:str, check_active_flag:bool = False) -> Encryptor:
        enc_name = enc_name.upper()
        return self.db_engine.get_encryptor_by_name(enc_name, check_active_flag)
 
    def list_encryptors(self, check_active_flag:bool = False) -> [Encryptor]:
        if check_active_flag:
            all_encryptors = self.db_engine.get_active_encryptors()
        else:
            all_encryptors = self.db_engine.get_all_encryptors()
        return [
                {
                    'ENC_NAME': enc.ENC_NAME,
                    'ENC_ACTIVE_FLAG': enc.ENC_ACTIVE_FLAG
                }
                for enc in all_encryptors
            ]

    def delete_encryptor(self, enc_name:str, hard:bool = False) -> None:
        self.db_engine.delete_encryptor_by_name(enc_name, hard)

    def recover_encryptor(self, enc_name:str) -> None:
        self.db_engine.recover_encryptor_by_name(enc_name)

    def flush_encryptors(self) -> None:
        self.db_engine.clean_up_inactive_encryptors()

    def purge_all_encryptors(self) -> None:
        self.db_engine.clean_up_all_encryptors()
