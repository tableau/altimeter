"""Table definitions for Result and ResultSet"""
import uuid

from sqlalchemy import Column, Index, Integer, ForeignKey, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from altimeter.qj.db.base_class import BASE
from altimeter.qj.models.job import Job

# pylint: disable=too-few-public-methods
class ResultSet(BASE):
    """ResultSet table definition. A ResultSet represents all results for a run of a given
    JobVersion."""

    __tablename__ = "result_set"

    id = Column(Integer, primary_key=True)
    result_set_id = Column(UUID(as_uuid=True), default=uuid.uuid4, nullable=False, unique=True)
    job_id = Column("job_id", ForeignKey(Job.id, ondelete="CASCADE"), nullable=False)
    job = relationship(Job)
    created = Column(DateTime, nullable=False)
    graph_spec = Column(JSONB, nullable=False)
    results = relationship("Result", passive_deletes=True)

    __table_args__ = (
        Index("result_set_job_id_idx", job_id,),
        Index("result_set_created_idx", created,),
    )


class Result(BASE):
    """Result table definition. A Result represents the single result of a Job run"""

    __tablename__ = "result"

    id = Column(Integer, primary_key=True)
    result_set_id = Column(
        "result_set_id", ForeignKey(ResultSet.id, ondelete="CASCADE"), nullable=False,
    )
    result_set = relationship(ResultSet)
    account_id = Column(Text, nullable=False)
    result_id = Column(UUID(as_uuid=True), default=uuid.uuid4, nullable=False, unique=True)
    result = Column(JSONB, nullable=False)

    __table_args__ = (
        Index("result_result_set_id_idx", result_set_id,),
        Index("result_account_id_idx", account_id),
    )
