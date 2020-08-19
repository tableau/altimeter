"""DB utility functions for testing"""
from contextlib import contextmanager
from typing import Generator
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from altimeter.qj.config import DBConfig


@contextmanager
def temp_db_session() -> Generator[Session, None, None]:
    """Return a db session which will be rolled back after the context is exited"""
    db_config = DBConfig()
    engine = create_engine(db_config.get_db_uri(), pool_pre_ping=True, pool_recycle=3600)
    connection = engine.connect()
    txn = connection.begin()
    session = Session(bind=connection)
    with patch("altimeter.qj.api.deps.SessionGenerator.get_session") as mock_get_session:
        mock_get_session.return_value = session
        try:
            yield session
        finally:
            session.close()
            txn.rollback()
            connection.close()
