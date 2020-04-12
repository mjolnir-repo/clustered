import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from ..env import env

class DatabaseEngine:
    def __init__(self):
        self.db_engine = create_engine(env.DATABASE_URL)

    @contextmanager
    def session_scope(self):
        _Session = sessionmaker(bind=self.db_engine, expire_on_commit=False)
        session  = _Session()
        try:
            yield session
        except Exception as e:
            print("ERROR: Exception occured while database session was open.")
            print(str(e))
            session.rollback()
        else:
            print("INFO: Database operations completed successfully.")
            session.commit()
        finally:
            # print("DEBUG: Expunging objects from session.")
            # session.expunge_all()
            session.close()
