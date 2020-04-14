import pickle
from cryptography.fernet import Fernet



class Encrypt:
    def __init__(self):
        pass

    @staticmethod
    def get_hashed_key(__key = Fernet.generate_key()):
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

    @classmethod
    def decrypt(cls, __encrypted_text, __hashed_key):
        __encryptor = cls.__get_encryptor_suite(__hashed_key)
        __decrypted_hashed_text = __encryptor.decrypt(__encrypted_text)
        return cls.__unhash(__decrypted_hashed_text)
