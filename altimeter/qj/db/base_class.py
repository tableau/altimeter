"""Base SQLAlchemy table class - all declarative tables should inherit from this."""
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()
