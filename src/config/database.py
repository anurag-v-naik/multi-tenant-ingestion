from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
import os
import logging
from typing import Dict, Generator

logger = logging.getLogger(__name__)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_size=settings.DATABASE_POOL_SIZE,
    max_overflow=settings.DATABASE_MAX_OVERFLOW,
)

async def get_database() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

class MultiTenantDatabase:
    def __init__(self):
        self.engines: Dict[str, any] = {}
        self.sessions: Dict[str, any] = {}
        self.base_db_url = os.getenv("BASE_DATABASE_URL", "postgresql://admin:admin123@localhost:5432")

    def create_tenant_database(self, tenant_id: str) -> bool:
        """Create a dedicated database for a tenant"""
        try:
            # Connect to default database to create new one
            admin_engine = create_engine(f"{self.base_db_url}/postgres")

            with admin_engine.connect() as conn:
                # Set autocommit for database creation
                conn.execute(text("COMMIT"))
                conn.execute(text(f"CREATE DATABASE {tenant_id}_db"))

            logger.info(f"Created database for tenant: {tenant_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to create database for tenant {tenant_id}: {e}")
            return False

    def get_tenant_engine(self, tenant_id: str):
        """Get or create database engine for specific tenant"""
        if tenant_id not in self.engines:
            db_url = f"{self.base_db_url}/{tenant_id}_db"
            self.engines[tenant_id] = create_engine(
                db_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                echo=False
            )

            # Create session factory
            self.sessions[tenant_id] = sessionmaker(
                bind=self.engines[tenant_id],
                autocommit=False,
                autoflush=False
            )

        return self.engines[tenant_id]

    @contextmanager
    def get_tenant_session(self, tenant_id: str) -> Generator:
        """Get database session for specific tenant with context manager"""
        engine = self.get_tenant_engine(tenant_id)
        SessionLocal = self.sessions[tenant_id]
        session = SessionLocal()

        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error for tenant {tenant_id}: {e}")
            raise
        finally:
            session.close()

    def validate_tenant_isolation(self, tenant_id: str) -> bool:
        """Validate that tenant data is properly isolated"""
        try:
            with self.get_tenant_session(tenant_id) as session:
                # Check if we can only access tenant-specific data
                result = session.execute(text("SELECT current_database()"))
                current_db = result.scalar()
                expected_db = f"{tenant_id}_db"

                return current_db == expected_db

        except Exception as e:
            logger.error(f"Tenant isolation validation failed for {tenant_id}: {e}")
            return False

    def initialize_tenant_schema(self, tenant_id: str, schema_models):
        """Initialize database schema for new tenant"""
        try:
            engine = self.get_tenant_engine(tenant_id)

            # Create all tables for this tenant
            for model in schema_models:
                model.metadata.create_all(bind=engine)

            logger.info(f"Initialized schema for tenant: {tenant_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize schema for tenant {tenant_id}: {e}")
            return False

    def drop_tenant_database(self, tenant_id: str) -> bool:
        """Drop tenant database (use with caution)"""
        try:
            # Close existing connections
            if tenant_id in self.engines:
                self.engines[tenant_id].dispose()
                del self.engines[tenant_id]
                del self.sessions[tenant_id]

            # Connect to default database to drop tenant database
            admin_engine = create_engine(f"{self.base_db_url}/postgres")

            with admin_engine.connect() as conn:
                conn.execute(text("COMMIT"))
                conn.execute(text(f"DROP DATABASE IF EXISTS {tenant_id}_db"))

            logger.info(f"Dropped database for tenant: {tenant_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to drop database for tenant {tenant_id}: {e}")
            return False


# Global database manager instance
db_manager = MultiTenantDatabase()


# Helper function for FastAPI dependency injection
def get_tenant_db_session(tenant_id: str):
    return db_manager.get_tenant_session(tenant_id)
