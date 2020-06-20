import pytest
import json
import traceback
import sys
import os
from clustered.engine.repository_engine import RepositoryEngine
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

@pytest.fixture(name="repo_engine", scope="module")
def _repo_engine(default_environment_configuration_file):
    return RepositoryEngine(default_environment_configuration_file)

@pytest.fixture(name="first_arg_set", scope="module")
def _first_arg_set(first_repository_name, first_encryptor, first_aws_key_set, default_repository_configuration_file):
    return {
        'repo_name': first_repository_name
        , 'enc_name': first_encryptor.ENC_NAME
        , **first_aws_key_set
        , 'repo_config_filepath': default_repository_configuration_file
    }

@pytest.fixture(name="second_arg_set", scope="module")
def _second_arg_set(second_repository_name, second_encryptor, second_aws_key_set, default_repository_configuration_file):
    return {
        'repo_name': second_repository_name
        , 'enc_name': second_encryptor.ENC_NAME
        , **second_aws_key_set
        , 'repo_config_filepath': default_repository_configuration_file
    }

def test_add_repository(db_engine, repo_engine, first_arg_set, second_arg_set):
    repo_engine.add_repository(**first_arg_set)
    with pytest.raises(RepositoryAlreadyExistsError):
        repo_engine.add_repository(**first_arg_set)
    repo_engine.add_repository(**second_arg_set)
    with db_engine.session_scope() as sess:
        repository_count = sess.query(Repository).count()
    assert repository_count == 2

def test_describe_repository(repo_engine, first_repository_name):
    repository = repo_engine.describe_repository(first_repository_name)
    assert repository.REPO_NAME == first_repository_name
    assert isinstance(repository, Repository)
    with pytest.raises(RepositoryNotPresentError):
        repo_engine.describe_repository('anything')
    with pytest.raises(TypeError):
        repo_engine.describe_repository()

def test_list_all_repositories(repo_engine, first_repository_name, second_repository_name):
    repositories = repo_engine.list_repositories()
    assert len(repositories) == 2
    repo_names = []
    for repository in repositories:
        assert list(repository.keys()) == ["REPO_NAME", "REPO_ACTIVE_FLAG"]
        repo_names.append(repository['REPO_NAME'])
    assert repo_names == [first_repository_name, second_repository_name]

def test_delete_repository(repo_engine, second_arg_set, second_repository_name):
    with pytest.raises(TypeError):
        repo_engine.delete_repository()
    repo_engine.delete_repository(second_repository_name, True)
    repositories = repo_engine.list_repositories()
    assert len(repositories) == 1
    repo_engine.add_repository(**second_arg_set)
    repo_engine.delete_repository(second_repository_name)
    repository = repo_engine.describe_repository(second_repository_name)
    assert repository.REPO_ACTIVE_FLAG == 'N'
    repositories = repo_engine.list_repositories(True)
    assert len(repositories) == 1
    with pytest.raises(RepositoryNotActiveError):
        repo_engine.describe_repository(second_repository_name, True)

def test_list_active_repositories(repo_engine, first_repository_name):
    repositories = repo_engine.list_repositories(True)
    assert len(repositories) == 1
    repo_names = []
    for repository in repositories:
        assert list(repository.keys()) == ["REPO_NAME", "REPO_ACTIVE_FLAG"]
        repo_names.append(repository['REPO_NAME'])
    assert repo_names == [first_repository_name]

def test_recover_repository(repo_engine, second_repository_name):
    with pytest.raises(TypeError):
        repo_engine.recover_repository()
    repo_engine.recover_repository(second_repository_name)
    assert repo_engine.describe_repository(second_repository_name, True)
    repositories = repo_engine.list_repositories()
    assert len(repositories) == 2
    repositories = repo_engine.list_repositories()
    assert len(repositories) == 2
    with pytest.raises(WrongActionInvocationError):
        repo_engine.recover_repository(second_repository_name)

def test_flush_repositories(repo_engine, second_repository_name):
    repo_engine.delete_repository(second_repository_name)
    repo_engine.flush_repositories()
    with pytest.raises(RepositoryNotPresentError):
        repo_engine.describe_repository(second_repository_name)
    all_repositories = repo_engine.list_repositories()
    assert len(all_repositories) == 1
    active_repositories = repo_engine.list_repositories(True)
    assert len(active_repositories) == 1

def test_clean_up_all_repositories(repo_engine, first_repository_name, second_repository_name):
    repo_engine.purge_all_repositories()
    with pytest.raises(RepositoryNotPresentError):
        repo_engine.describe_repository(first_repository_name)
        repo_engine.describe_repository(second_repository_name)
        repo_engine.list_repositories()
        repo_engine.list_repositories(True)
