"""
Database configuration. Single source of truth: DATABASE_URL.

- Default: embedded SQLite at server/topology.db (no setup).
- PostgreSQL: set DATABASE_URL=postgresql://user:pass@host/dbname and install
  the driver: uv sync --extra postgres
"""
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, text
from sqlalchemy.sql import func

# Default: SQLite in server directory. Override with DATABASE_URL for PostgreSQL.
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    db_path = Path(__file__).resolve().parent / "topology.db"
    DATABASE_URL = f"sqlite:///{db_path}"

_is_sqlite = DATABASE_URL.startswith("sqlite")
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if _is_sqlite else {},
    echo=os.environ.get("SQL_ECHO", "").lower() in ("1", "true"),
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# JSON column: SQLite uses TEXT, PostgreSQL uses JSONB (when using postgres dialect)
SnapshotData = JSON().with_variant(JSONB(), "postgresql")


class Cluster(Base):
    __tablename__ = "clusters"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    bootstrap_servers = Column(String(512), nullable=False)
    schema_registry_url = Column(String(512), nullable=True)
    connect_url = Column(String(512), nullable=True)
    jmx_host = Column(String(255), nullable=True)
    jmx_port = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_id = Column(Integer, nullable=False)
    data = Column(SnapshotData, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ClusterRegistration(Base):
    """Optional app registrations: producer, consumer, or streams (no interceptors needed)."""
    __tablename__ = "cluster_registrations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_id = Column(Integer, nullable=False)
    app_name = Column(String(255), nullable=False)
    role = Column(String(32), nullable=False)  # "producer", "consumer", or "streams"
    topics = Column(SnapshotData, nullable=False)  # input topics (consumesFrom / producesTo for producer)
    output_topics = Column(SnapshotData, nullable=True)  # for streams: producesTo (JSON array)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create tables if they don't exist. Same schema for SQLite and PostgreSQL."""
    Base.metadata.create_all(bind=engine)
    
    # Migrations for existing databases
    with engine.connect() as conn:
        # Add output_topics to cluster_registrations if missing
        try:
            if _is_sqlite:
                conn.execute(text("ALTER TABLE cluster_registrations ADD COLUMN output_topics TEXT"))
            else:
                conn.execute(text("ALTER TABLE cluster_registrations ADD COLUMN IF NOT EXISTS output_topics JSONB"))
            conn.commit()
        except Exception:
            pass  # column already exists
        
        # Add JMX columns to clusters if missing
        try:
            if _is_sqlite:
                conn.execute(text("ALTER TABLE clusters ADD COLUMN jmx_host VARCHAR(255)"))
                conn.execute(text("ALTER TABLE clusters ADD COLUMN jmx_port INTEGER"))
            else:
                conn.execute(text("ALTER TABLE clusters ADD COLUMN IF NOT EXISTS jmx_host VARCHAR(255)"))
                conn.execute(text("ALTER TABLE clusters ADD COLUMN IF NOT EXISTS jmx_port INTEGER"))
            conn.commit()
        except Exception:
            pass  # columns already exist
