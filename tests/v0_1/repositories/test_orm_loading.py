import os
import pprint
import re
import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Engine, create_engine, inspect, text

from v0_1.infrastructure.database.orm.base import Base

import v0_1.infrastructure.database.orm.prisma.investigador.models
import v0_1.infrastructure.database.orm.prisma.investigador.views
import v0_1.infrastructure.database.orm.prisma.publicacion.models
import v0_1.infrastructure.database.orm.prisma.publicacion.views
import v0_1.infrastructure.database.orm.prisma.metrica.models
import v0_1.infrastructure.database.orm.prisma.misc.models
import v0_1.infrastructure.database.orm.prisma.misc.views
import v0_1.infrastructure.database.orm.prisma_resultado.models
import v0_1.infrastructure.database.orm.api.models
import v0_1.infrastructure.database.orm.config.models
import v0_1.infrastructure.database.orm.prisma_cvn.models
import v0_1.infrastructure.database.orm.prisma_erasmus_plus.models
import v0_1.infrastructure.database.orm.prisma_proyectos.models

DUMP_FILE_PATHS = {
    "prisma": "tests/v0_1/repositories/dumps/dump-prisma.sql",
    "prisma_resultado": "tests/v0_1/repositories/dumps/dump-prisma_resultado.sql",
    "api": "tests/v0_1/repositories/dumps/dump-api.sql",
    "config": "tests/v0_1/repositories/dumps/dump-config.sql",
    "prisma_cvn": "tests/v0_1/repositories/dumps/dump-prisma_cvn.sql",
    "prisma_erasmus_plus": "tests/v0_1/repositories/dumps/dump-prisma_erasmus_plus.sql",
    "prisma_proyectos": "tests/v0_1/repositories/dumps/dump-prisma_proyectos.sql",
}


def load_sql_dump_native(engine: Engine, dump_file_path: str, database: str) -> None:
    """
    Reads and executes a MariaDB SQL dump file directly using SQLAlchemy,
    properly tracking custom DELIMITER statements and stripping empty/comment lines.
    """
    if not os.path.exists(dump_file_path):
        pytest.fail(f"SQL dump file not found at: {dump_file_path}")

    with open(dump_file_path, "r", encoding="utf-8") as f:
        dump_text = f.read()

    # 1. Remove MySQL conditional comments like /*!40101 SET ... */;
    dump_text = re.sub(r"/\*!\d+.*?\*/;?", "", dump_text, flags=re.DOTALL)

    # 2. Parse statements respecting DELIMITER directives (e.g., DELIMITER //)
    statements = []
    current_delimiter = ";"
    buffer = []

    for line in dump_text.splitlines():
        stripped_line = line.strip()

        # Skip full-line comments
        if stripped_line.startswith(("--", "#", "/*")) and not stripped_line.endswith(
            "*/"
        ):
            continue

        # Detect DELIMITER directive changes
        delim_match = re.match(r"^\s*DELIMITER\s+(\S+)", line, re.IGNORECASE)
        if delim_match:
            current_delimiter = delim_match.group(1)
            continue

        buffer.append(line)
        joined_buffer = "\n".join(buffer)

        # Check if the cumulative buffer ends with the active delimiter
        if joined_buffer.rstrip().endswith(current_delimiter):
            stmt = joined_buffer.rstrip()[: -len(current_delimiter)].strip()
            stmt = re.sub(r"^;+", "", stmt).strip()

            if stmt:
                statements.append(stmt)
            buffer = []

    if buffer:
        stmt = "\n".join(buffer).strip()
        stmt = re.sub(r"^;+", "", stmt).strip()
        if stmt:
            statements.append(stmt)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"USE `{database}`"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        try:
            for cleaned in statements:
                cleaned_stmt = cleaned.strip(";\n\r\t ")
                if not cleaned_stmt:
                    continue

                try:
                    conn.execute(text(cleaned))
                except Exception as err:
                    print(
                        f"\n[Dump Load Warning] Statement: {cleaned[:60]}... -> {err}"
                    )
        finally:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))


def test_mariadb_dump(engine: Engine):
    if engine.dialect.name not in ("mysql", "mariadb"):
        pytest.skip("This test is specific to MariaDB.")

    # 1. Clear & recreate all target databases
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        result = conn.execute(text("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys', 'test')
        """))
        existing_user_dbs = [row[0] for row in result]

        for db in existing_user_dbs:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{db}`"))

        for db in DUMP_FILE_PATHS.keys():
            conn.execute(text(f"CREATE DATABASE `{db}`"))

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

    # 2. Create Engine instance attached to default DB
    db_url = f"{engine.url.render_as_string(hide_password=False)}/prisma"
    prisma_engine = create_engine(db_url)

    try:
        # 3. Load all dump files into their respective databases
        for database, dump_file_path in DUMP_FILE_PATHS.items():
            load_sql_dump_native(prisma_engine, dump_file_path, database)

        # 4. Fallback schema assignment for ORM models missing an explicit schema
        for table in Base.metadata.tables.values():
            if table.schema is None:
                table.schema = "prisma"

        # 5. Perform reflection comparison across multiple schemas
        with prisma_engine.connect() as conn:
            target_schemas = list(DUMP_FILE_PATHS.keys())

            def include_name(name, type_, parent_names):
                if type_ == "schema":
                    return name in target_schemas
                return True

            def include_object(object_, name, type_, reflected, compare_to):
                if type_ == "table":
                    # Determine schema cleanly whether attached or default
                    schema = object_.schema or "prisma"
                    return schema in target_schemas
                return True

            # Use include_name along with include_schemas=True
            context = MigrationContext.configure(
                conn,
                opts={
                    "include_schemas": True,
                    "target_metadata": Base.metadata,
                    "include_name": include_name,
                    "include_object": include_object,
                    "compare_type": True,
                },
            )

            diff = compare_metadata(context, Base.metadata)

            structural_errors = [
                op
                for op in diff
                if op[0] in ("add_column", "remove_column")
                or (
                    any(
                        _op[0] in ("modify_type", "modify_nullable", "modify_default")
                        for _op in op
                    )
                    if isinstance(op, list)
                    else False
                )
            ]

            assert (
                not structural_errors
            ), f"Structural differences found between dump and ORM: {pprint.pformat(structural_errors)}"

    finally:
        prisma_engine.dispose()


def test_inspect(engine: Engine):
    inspect(engine)
