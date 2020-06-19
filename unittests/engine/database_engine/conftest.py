import pytest
import json
import os
import sys
import traceback
from clustered.engine.database_engine import db_engine
from clustered.models import Base


@pytest.yield_fixture(name="db_engine", scope="module", autouse=True)
def _db_engine_object():
    """ 
        Setting up test database.
        Yielding the db engine object
        Tearing down the test database
    """
    try:
        Base.metadata.create_all(db_engine.engine)
        yield db_engine
        Base.metadata.drop_all(db_engine.engine)
    except Exception as e:
        print("~" * 100)
        traceback.print_exc(file=sys.stdout)
        print("~" * 100)
        assert False