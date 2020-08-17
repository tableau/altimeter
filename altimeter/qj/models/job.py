"""Job related table definition"""
# pylint: disable=too-few-public-methods

from sqlalchemy import Boolean, Column, DateTime, Enum, Index, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from altimeter.qj.db.base_class import BASE
from altimeter.qj.schemas.job import Category, Severity


class Job(BASE):
    """Job table definition"""

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=False)
    graph_spec = Column(JSONB, nullable=False)
    query_fields = Column(JSONB, nullable=False)
    category = Column(Enum(Category), nullable=False)
    severity = Column(Enum(Severity), nullable=False)
    query = Column(Text, nullable=False)
    max_graph_age_sec = Column(Integer, nullable=False)
    created = Column(DateTime, nullable=False)
    active = Column(Boolean, nullable=False, server_default="false")
    result_expiration_sec = Column(Integer, nullable=False)
    max_result_age_sec = Column(Integer, nullable=False)
    result_sets = relationship("ResultSet", passive_deletes=True)

    __table_args__ = (
        Index("job_name_active_key", name, active, unique=True, postgresql_where=(active)),
        UniqueConstraint(name, created, name="job_name_created_key"),
    )
