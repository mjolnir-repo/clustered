import pytest
import json
import traceback
import sys
import os
from clustered.models import Encryptor
from clustered.exceptions import EncryptorNotPresentError, EncryptorNotActiveError, EncryptorAlreadyExistsError, WrongActionInvocationError, RepositoryNotPresentError, RepositoryNotActiveError, RepositoryAlreadyExistsError, EncryptorNotPresentError



def test_add_encryptor(db_engine, first_encryptor_name, second_encryptor_name):
    db_engine.add_encryptor(first_encryptor_name)
    with pytest.raises(EncryptorAlreadyExistsError):
        db_engine.add_encryptor(first_encryptor_name)
    db_engine.add_encryptor(second_encryptor_name)
    with db_engine.session_scope() as sess:
        encryptor_count = sess.query(Encryptor).count()
    assert encryptor_count == 2

def test_get_encryptor_by_name(db_engine, first_encryptor_name):
    encryptor = db_engine.get_encryptor_by_name(first_encryptor_name)
    assert encryptor.ENC_NAME == first_encryptor_name
    assert isinstance(encryptor, Encryptor)
    with pytest.raises(EncryptorNotPresentError):
        db_engine.get_encryptor_by_name('anything')
    with pytest.raises(TypeError):
        db_engine.get_encryptor_by_name()

def test_get_all_encryptor(db_engine):
    encryptors = db_engine.get_all_encryptors()
    assert len(encryptors) == 2
    for encryptor in encryptors:
        assert isinstance(encryptor, Encryptor)

def test_delete_encryptor_by_name(db_engine, second_encryptor_name):
    with pytest.raises(TypeError):
        db_engine.delete_encryptor_by_name()    
    db_engine.delete_encryptor_by_name(second_encryptor_name)
    encryptor = db_engine.get_encryptor_by_name(second_encryptor_name)
    assert encryptor.ENC_ACTIVE_FLAG == 'N'
    encryptors = db_engine.get_all_encryptors()
    assert len(encryptors) == 2

def test_get_active_encryptors(db_engine):
    encryptors = db_engine.get_active_encryptors()
    assert len(encryptors) == 1
    for encryptor in encryptors:
        assert isinstance(encryptor, Encryptor)

def test_recover_encryptor_by_name(db_engine, second_encryptor_name):
    with pytest.raises(TypeError):
        db_engine.recover_encryptor_by_name()    
    db_engine.recover_encryptor_by_name(second_encryptor_name)
    encryptor = db_engine.get_encryptor_by_name(second_encryptor_name)
    assert encryptor.ENC_ACTIVE_FLAG == 'Y'
    all_encryptors = db_engine.get_all_encryptors()
    assert len(all_encryptors) == 2
    active_encryptors = db_engine.get_active_encryptors()
    assert len(active_encryptors) == 2
    with pytest.raises(WrongActionInvocationError):
        db_engine.recover_encryptor_by_name(second_encryptor_name)

def test_clean_up_inactive_encryptors(db_engine, second_encryptor_name):
    db_engine.delete_encryptor_by_name(second_encryptor_name)
    db_engine.clean_up_inactive_encryptors()
    with pytest.raises(EncryptorNotPresentError):
        db_engine.get_encryptor_by_name(second_encryptor_name)
    all_encryptors = db_engine.get_all_encryptors()
    assert len(all_encryptors) == 1
    active_encryptors = db_engine.get_active_encryptors()
    assert len(active_encryptors) == 1

def test_clean_up_all_encryptors(db_engine, first_encryptor_name, second_encryptor_name):
    db_engine.clean_up_all_encryptors()
    with pytest.raises(EncryptorNotPresentError):
        db_engine.get_encryptor_by_name(first_encryptor_name)
        db_engine.get_encryptor_by_name(second_encryptor_name)
        db_engine.get_all_encryptors()
        db_engine.get_active_encryptors()
