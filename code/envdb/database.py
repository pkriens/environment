"""
Database connection and session management for EnvDB.
"""

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from typing import Optional, Generator
import logging

from .models import Base

logger = logging.getLogger(__name__)


class Database:
    """Database connection manager for environmental monitoring data."""
    
    def __init__(self, database_url: str, echo: bool = False):
        """
        Initialize database connection.
        
        Args:
            database_url: SQLAlchemy database URL
            echo: Whether to log all SQL statements
        """
        self.database_url = database_url
        self.engine = create_engine(database_url, echo=echo)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Enable SQLite performance optimizations
        if 'sqlite' in database_url.lower():
            self._configure_sqlite()
    
    def _configure_sqlite(self):
        """Configure SQLite-specific performance settings."""
        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            # Enable WAL mode for better concurrency
            cursor.execute("PRAGMA journal_mode=WAL")
            # Optimize for bulk inserts
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA cache_size=10000")
            cursor.execute("PRAGMA temp_store=MEMORY")
            # Enable foreign key constraints
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    
    def create_all_tables(self, drop_existing: bool = False):
        """
        Create all database tables.
        
        Args:
            drop_existing: Whether to drop existing tables first
        """
        if drop_existing:
            Base.metadata.drop_all(self.engine)
        
        Base.metadata.create_all(self.engine)
        logger.info("Created all database tables")
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.
        
        Yields:
            SQLAlchemy session
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def get_session_direct(self) -> Session:
        """
        Get a session directly (caller must manage commit/rollback/close).
        
        Returns:
            SQLAlchemy session
        """
        return self.SessionLocal()
    
    def optimize_for_bulk_operations(self):
        """Optimize database settings for bulk data loading."""
        with self.engine.connect() as conn:
            if 'sqlite' in self.database_url.lower():
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("PRAGMA journal_mode=MEMORY")
                conn.execute("PRAGMA cache_size=100000")
            elif 'postgresql' in self.database_url.lower():
                conn.execute("SET synchronous_commit TO OFF")
                conn.execute("SET checkpoint_segments = 32")
            
    def restore_normal_operations(self):
        """Restore normal database settings after bulk operations."""
        with self.engine.connect() as conn:
            if 'sqlite' in self.database_url.lower():
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA journal_mode=WAL")
            elif 'postgresql' in self.database_url.lower():
                conn.execute("SET synchronous_commit TO ON")
    
    def create_partitions(self, table_name: str, partition_type: str = 'monthly'):
        """
        Create time-based partitions for large tables (PostgreSQL only).
        
        Args:
            table_name: Name of table to partition
            partition_type: 'monthly' or 'yearly'
        """
        if 'postgresql' not in self.database_url.lower():
            logger.warning("Partitioning only supported for PostgreSQL")
            return
            
        # Implementation would be database-specific
        # This is a placeholder for partition creation logic
        logger.info(f"Creating {partition_type} partitions for {table_name}")
    
    def get_table_stats(self) -> dict:
        """
        Get statistics about table sizes and row counts.
        
        Returns:
            Dictionary with table statistics
        """
        stats = {}
        
        with self.get_session() as session:
            for table in Base.metadata.tables:
                try:
                    count_query = session.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[table] = count_query.scalar()
                except Exception as e:
                    stats[table] = f"Error: {e}"
        
        return stats