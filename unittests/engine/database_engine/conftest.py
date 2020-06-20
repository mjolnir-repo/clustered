import pytest
import json
import os
import sys
import traceback
from clustered.models import Base
from clustered.engine.database_engine import DatabaseEngine


@pytest.yield_fixture(name="db_engine", scope="module", autouse=True)
def _db_engine_object(default_environment_configuration_file):
    """ 
        Setting up test database.
        Yielding the db engine object
        Tearing down the test database
    """
    try:
        db_engine = DatabaseEngine(default_environment_configuration_file)
        Base.metadata.create_all(db_engine.engine)
        yield db_engine
        Base.metadata.drop_all(db_engine.engine)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False