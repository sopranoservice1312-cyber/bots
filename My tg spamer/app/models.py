from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, BigInteger, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base


class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    phone = Column(String(32), unique=True, nullable=False)
    name = Column(String(64), nullable=True)
    session_path = Column(String(255), unique=True, nullable=False)
    is_authorized = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    api_id = Column(Integer, nullable=True)
    api_hash = Column(String(64), nullable=True)
    phone_code_hash = Column(String(128), nullable=True)

    logs = relationship("MessageLog", back_populates="account", cascade="all, delete-orphan")


class Template(Base):
    __tablename__ = "templates"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(128), nullable=False)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    status = Column(String(32), default="queued")
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="SET NULL"), nullable=True)
    targets_blob = Column(Text, nullable=False)
    template_ids_blob = Column(Text, nullable=False)
    randomize = Column(Boolean, default=True)
    schedule_at = Column(DateTime(timezone=True), nullable=True)
    context_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    error = Column(Text, nullable=True)
    attempts = Column(Integer, default=0)
    account_ids_blob = Column(Text, nullable=True)

    # cyclic send fields
    is_cyclic = Column(Boolean, default=False)
    cycle_minutes = Column(Integer, nullable=True)
    paused = Column(Boolean, default=False)
    stopped = Column(Boolean, default=False)
    next_run_at = Column(DateTime(timezone=True), nullable=True)

    # связь с логами
    logs = relationship("MessageLog", back_populates="job", cascade="all, delete-orphan")


class MessageLog(Base):
    __tablename__ = "message_logs"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="SET NULL"), index=True, nullable=True)
    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    target = Column(String(256), index=True)
    template_id = Column(Integer, ForeignKey("templates.id", ondelete="SET NULL"), nullable=True)
    status = Column(String(32), default="queued")
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    peer_id = Column(BigInteger, nullable=True)
    access_hash = Column(BigInteger, nullable=True)

    account = relationship("Account", back_populates="logs")
    job = relationship("Job", back_populates="logs")
