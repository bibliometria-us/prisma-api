# tests/conftest.py
from typing import Generator

import pytest
import redis
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from v0_1.infrastructure.database.models.base import Base

# Connection URLs
TEST_MARIADB_URL = "mariadb+pymysql://root:rootpass@prisma_mariadb_test:3306"
TEST_POSTGRES_URL = "postgresql+psycopg://postgres:postgres@postgres_test:5432"
TEST_REDIS_URL = "redis://redis:6379/15"

DATABASES = ["test", "prisma"]


def setup_databases(engine, dialect_name: str):
    """Handles engine-specific database and schema creation setup."""
    if dialect_name in ("mariadb", "mysql"):
        for db_name in DATABASES:
            with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))

    elif dialect_name == "postgresql":
        # Create schemas in PostgreSQL if defined in model __table_args__
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            for schema_name in DATABASES:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))


@pytest.fixture(
    scope="session", params=["mariadb", "postgres"], ids=["MariaDB", "Postgres"]
)
def engine(request):
    """
    Parametrized session-scoped fixture.
    Creates and yields an engine for each database target.
    """
    db_type = request.param

    if db_type == "mariadb":
        url = TEST_MARIADB_URL
    else:
        url = TEST_POSTGRES_URL

    engine = create_engine(url)
    dialect_name = engine.dialect.name

    # 1. Create target databases/schemas
    setup_databases(engine, dialect_name)

    # 2. Create tables
    Base.metadata.create_all(bind=engine)

    yield engine

    # 3. Cleanup on session teardown
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(engine):
    """
    Provide an isolated DB session for each test function.
    Rolls back all inserts/updates/deletes automatically after each test.
    """
    connection = engine.connect()
    transaction = connection.begin()

    Session = sessionmaker(bind=connection, expire_on_commit=False)
    session = Session()

    # Create a nested transaction (savepoint)
    session.begin_nested()

    @event.listens_for(session, "after_transaction_end")
    def restart_savepoint(session, transaction):
        """Re-establish savepoint if code explicitly called session.commit()."""
        if transaction.nested and not transaction._parent.nested:
            session.begin_nested()

    yield session

    session.close()

    if transaction.is_active:
        transaction.rollback()

    connection.close()


@pytest.fixture(scope="function")
def redis_client() -> Generator[redis.Redis, None, None]:
    """
    Provides a Redis client for testing and automatically flushes state
    after each test function for complete test isolation.
    """
    client = redis.Redis.from_url(TEST_REDIS_URL, decode_responses=False)

    yield client

    # Clean up Redis database after test execution without calling flushdb()
    try:
        keys = client.keys("*")
        if keys:
            client.delete(*keys)
    except (redis.RedisError, NotImplementedError, Exception):
        pass
