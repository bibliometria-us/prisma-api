import os
from alembic import context

from security.secrets import get_secret

config = context.config

prefix = os.getenv("DATABASE_URL_PREFIX")
user = get_secret("database_user")
password = get_secret("database_password")
host = os.getenv("DATABASE_HOST")
port = os.getenv("DATABASE_PORT")

if not all([prefix, user, password, host, port]):
    raise ValueError(
        "Database configuration is incomplete. Please set all required environment variables."
    )

db_url = f"{prefix}://{user}:{password}@{host}:{port}"

config.set_main_option("sqlalchemy.url", db_url)
