import pytest
import json
import traceback
import sys
import os
from clustered.models import Repository
from clustered.exceptions import RepositoryNotPresentError, RepositoryNotActiveError, RepositoryAlreadyExistsError, WrongActionInvocationError


@pytest.fixture(name='first_encryptor', scope="module")
def _first_encryptor(db_engine, first_encryptor_name):
    encryptor = db_engine.add_encryptor(first_encryptor_name)
    yield encryptor
    db_engine.delete_encryptor_by_name(first_encryptor_name, True)

@pytest.fixture(name='second_encryptor', scope="module")
def _second_encryptor(db_engine, second_encryptor_name):
    encryptor = db_engine.add_encryptor(second_encryptor_name)
    yield encryptor
    db_engine.delete_encryptor_by_name(second_encryptor_name, True)

def test_add_repository(db_engine, first_repository_name, first_encryptor, first_aws_key_set, second_repository_name, second_encryptor, second_aws_key_set):
    first_arg_set = {
        'repo_name': first_repository_name
        , 'enc_name': first_encryptor.ENC_NAME
        , **first_aws_key_set
    }
    second_arg_set = {
        'repo_name': second_repository_name
        , 'enc_name': second_encryptor.ENC_NAME
        , **second_aws_key_set
    }
    first_repository = db_engine.add_repository(**first_arg_set)
    assert isinstance(first_repository, Repository)
    assert first_repository.REPO_NAME == first_repository_name
    with pytest.raises(RepositoryAlreadyExistsError):
        db_engine.add_repository(**first_arg_set)
    second_repository = db_engine.add_repository(**second_arg_set)
    assert isinstance(second_repository, Repository)
    assert second_repository.REPO_NAME == second_repository_name
    with db_engine.session_scope() as sess:
        repository_count = sess.query(Repository).count()
    assert repository_count == 2

def test_get_repository_by_name(db_engine, first_repository_name):
    repository = db_engine.get_repository_by_name(first_repository_name)
    assert repository.REPO_NAME == first_repository_name
    assert isinstance(repository, Repository)
    with pytest.raises(RepositoryNotPresentError):
        db_engine.get_repository_by_name('anything')
    with pytest.raises(TypeError):
        db_engine.get_repository_by_name()

def test_get_all_repositories(db_engine):
    repositories = db_engine.get_all_repositories()
    assert len(repositories) == 2
    for repository in repositories:
        assert isinstance(repository, Repository)

def test_delete_repository_by_name(db_engine, second_repository_name):
    with pytest.raises(TypeError):
        db_engine.delete_repository_by_name()    
    db_engine.delete_repository_by_name(second_repository_name)
    repository = db_engine.get_repository_by_name(second_repository_name)
    assert repository.REPO_ACTIVE_FLAG == 'N'
    repositories = db_engine.get_all_repositories()
    assert len(repositories) == 2

def test_get_active_repositories(db_engine):
    repositories = db_engine.get_active_repositories()
    assert len(repositories) == 1
    for repository in repositories:
        assert isinstance(repository, Repository)

def test_recover_repository_by_name(db_engine, second_repository_name):
    with pytest.raises(TypeError):
        db_engine.recover_repository_by_name()
    with pytest.raises(RepositoryNotActiveError):
        db_engine.get_repository_by_name(second_repository_name, True)
    db_engine.recover_repository_by_name(second_repository_name)
    repository = db_engine.get_repository_by_name(second_repository_name)
    assert repository.REPO_ACTIVE_FLAG == 'Y'
    all_repositories = db_engine.get_all_repositories()
    assert len(all_repositories) == 2
    active_repositories = db_engine.get_active_repositories()
    assert len(active_repositories) == 2
    with pytest.raises(WrongActionInvocationError):
        db_engine.recover_repository_by_name(second_repository_name)

def test_clean_up_inactive_repositories(db_engine, second_repository_name):
    db_engine.delete_repository_by_name(second_repository_name)
    db_engine.clean_up_inactive_repositories()
    with pytest.raises(RepositoryNotPresentError):
        db_engine.get_repository_by_name(second_repository_name)
    all_repositories = db_engine.get_all_repositories()
    assert len(all_repositories) == 1
    active_repositories = db_engine.get_active_repositories()
    assert len(active_repositories) == 1

def test_clean_up_all_repositories(db_engine, first_repository_name, second_repository_name):
    db_engine.clean_up_all_repositories()
    with pytest.raises(RepositoryNotPresentError):
        db_engine.get_repository_by_name(first_repository_name)
        db_engine.get_repository_by_name(second_repository_name)
        db_engine.get_all_repositories()
        db_engine.get_active_repositories()
