# app/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
import db.claves as config


class Base(DeclarativeBase):
    pass


DATABASE_URL = (
    f"mariadb+pymysql://{config.db_user}:{config.db_password}@mariadb:3306/prisma"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Recommended for MariaDB to handle dropped connections
    pool_recycle=3600,  # Prevents "MySQL server has gone away" errors
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
