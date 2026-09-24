# app/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import db.claves as config

MARIADB_URL = f"mariadb+pymysql://{config.db_user}:{config.db_password}@mariadb:3306/"

mariadb_engine = create_engine(
    MARIADB_URL,
    pool_pre_ping=True,  # Recommended for MariaDB to handle dropped connections
    pool_recycle=3600,  # Prevents "MySQL server has gone away" errors
)

MariaDBSession = sessionmaker(autocommit=False, autoflush=False, bind=mariadb_engine)
