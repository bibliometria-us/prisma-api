import pytest
from db.conexion import BaseDatos


@pytest.fixture(scope="session")
def database():
    db = BaseDatos(
        autocommit=False, keep_connection_alive=True, database=None, test=True
    )
    db.startConnection()
    db.connection.start_transaction()
    yield db
    db.connection.rollback()
    db.connection.close()
