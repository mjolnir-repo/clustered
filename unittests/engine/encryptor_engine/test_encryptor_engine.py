import pytest
import json
import traceback
import sys
import os
from clustered.engine.encryptor_engine import EncryptorEngine
from clustered.models import Encryptor
from clustered.exceptions import EncryptorNotPresentError, EncryptorNotActiveError, EncryptorAlreadyExistsError, WrongActionInvocationError


@pytest.fixture(name="enc_engine", scope="module")
def _enc_engine(default_environment_configuration_file):
    return EncryptorEngine(default_environment_configuration_file)

def test_add_encryptor(db_engine, enc_engine, first_encryptor_name, second_encryptor_name):
    enc_engine.add_encryptor(first_encryptor_name)
    with pytest.raises(EncryptorAlreadyExistsError):
        enc_engine.add_encryptor(first_encryptor_name)
    enc_engine.add_encryptor(second_encryptor_name)
    with db_engine.session_scope() as sess:
        encryptor_count = sess.query(Encryptor).count()
    assert encryptor_count == 2

def test_describe_encryptor(enc_engine, first_encryptor_name):
    encryptor = enc_engine.describe_encryptor(first_encryptor_name)
    assert encryptor.ENC_NAME == first_encryptor_name
    assert isinstance(encryptor, Encryptor)
    with pytest.raises(EncryptorNotPresentError):
        enc_engine.describe_encryptor('anything')
    with pytest.raises(TypeError):
        enc_engine.describe_encryptor()

def test_list_all_encryptors(enc_engine, first_encryptor_name, second_encryptor_name):
    encryptors = enc_engine.list_encryptors()
    assert len(encryptors) == 2
    enc_names = []
    for encryptor in encryptors:
        assert list(encryptor.keys()) == ["ENC_NAME", "ENC_ACTIVE_FLAG"]
        enc_names.append(encryptor['ENC_NAME'])
    assert enc_names == [first_encryptor_name, second_encryptor_name]

def test_delete_encryptor(enc_engine, second_encryptor_name):
    with pytest.raises(TypeError):
        enc_engine.delete_encryptor()
    enc_engine.delete_encryptor(second_encryptor_name, True)
    encryptors = enc_engine.list_encryptors()
    assert len(encryptors) == 1
    enc_engine.add_encryptor(second_encryptor_name)
    enc_engine.delete_encryptor(second_encryptor_name)
    encryptor = enc_engine.describe_encryptor(second_encryptor_name)
    assert encryptor.ENC_ACTIVE_FLAG == 'N'
    encryptors = enc_engine.list_encryptors(True)
    assert len(encryptors) == 1
    with pytest.raises(EncryptorNotActiveError):
        enc_engine.describe_encryptor(second_encryptor_name, True)

def test_list_active_encryptors(enc_engine, first_encryptor_name):
    encryptors = enc_engine.list_encryptors(True)
    assert len(encryptors) == 1
    enc_names = []
    for encryptor in encryptors:
        assert list(encryptor.keys()) == ["ENC_NAME", "ENC_ACTIVE_FLAG"]
        enc_names.append(encryptor['ENC_NAME'])
    assert enc_names == [first_encryptor_name]

def test_recover_encryptor(enc_engine, second_encryptor_name):
    with pytest.raises(TypeError):
        enc_engine.recover_encryptor()
    enc_engine.recover_encryptor(second_encryptor_name)
    assert enc_engine.describe_encryptor(second_encryptor_name, True)
    encryptors = enc_engine.list_encryptors()
    assert len(encryptors) == 2
    encryptors = enc_engine.list_encryptors()
    assert len(encryptors) == 2
    with pytest.raises(WrongActionInvocationError):
        enc_engine.recover_encryptor(second_encryptor_name)

def test_flush_encryptors(enc_engine, second_encryptor_name):
    enc_engine.delete_encryptor(second_encryptor_name)
    enc_engine.flush_encryptors()
    with pytest.raises(EncryptorNotPresentError):
        enc_engine.describe_encryptor(second_encryptor_name)
    all_encryptors = enc_engine.list_encryptors()
    assert len(all_encryptors) == 1
    active_encryptors = enc_engine.list_encryptors(True)
    assert len(active_encryptors) == 1

def test_clean_up_all_encryptors(enc_engine, first_encryptor_name, second_encryptor_name):
    enc_engine.purge_all_encryptors()
    with pytest.raises(EncryptorNotPresentError):
        enc_engine.describe_encryptor(first_encryptor_name)
        enc_engine.describe_encryptor(second_encryptor_name)
        enc_engine.list_encryptors()
        enc_engine.list_encryptors(True)
