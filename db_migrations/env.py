import os
from alembic import context

from security.secrets import get_secret
import db.claves as db_config

config = context.config


prefix = db_config.db_url_prefix
user = db_config.db_user
password = db_config.db_password
host = db_config.db_host
port = db_config.db_port

if not all([prefix, user, password, host, port]):
    raise ValueError(
        "Database configuration is incomplete. Please set all required environment variables."
    )

db_url = f"{prefix}://{user}:{password}@{host}:{port}"

config.set_main_option("sqlalchemy.url", db_url)
