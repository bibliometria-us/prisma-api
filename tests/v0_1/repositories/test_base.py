# tests/test_models.py
import pytest
from typing import Optional, List, Sequence, Any
from sqlalchemy import String, ForeignKey, select, func, text, over
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as mariadb_insert
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.exc import IntegrityError, DBAPIError

from v0_1.database import Base
from v0_1.repositories.base import BaseRepository

# =====================================================================
# MODEL DECLARATIONS
# =====================================================================


class TestItem(Base):
    __tablename__ = "test_items"
    __table_args__ = {"schema": "test"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50))
    code: Mapped[str] = mapped_column(String(20), unique=True)
    category: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    price: Mapped[float] = mapped_column(default=0.0)
    is_active: Mapped[bool] = mapped_column(default=True)


class TestCompositeItem(Base):
    __tablename__ = "test_composite_items"
    __table_args__ = {"schema": "test"}

    tenant_id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(50))


class TestProject(Base):
    __tablename__ = "test_projects"
    __table_args__ = {"schema": "test"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50))

    tasks: Mapped[List["TestTask"]] = relationship(
        "TestTask", back_populates="project", cascade="all, delete-orphan"
    )


class TestTask(Base):
    __tablename__ = "test_tasks"
    __table_args__ = {"schema": "test"}

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), default="TODO")

    project_id: Mapped[int] = mapped_column(
        ForeignKey("test.test_projects.id", ondelete="CASCADE")
    )
    project: Mapped["TestProject"] = relationship("TestProject", back_populates="tasks")


# =====================================================================
# CUSTOM REPOSITORIES WITH ENGINE-SPECIFIC & COMPLEX QUERIES
# =====================================================================


class ItemRepository(BaseRepository[TestItem]):
    """Custom repository demonstrating raw SQL, dialect switching, and window functions."""

    def __init__(self, session):
        super().__init__(model=TestItem, session=session)

    def upsert_item(
        self, code: str, name: str, price: float, category: Optional[str] = None
    ) -> TestItem:
        """Custom UPSERT query targeting engine-specific syntax."""
        if self.dialect_name == "postgresql":
            stmt = pg_insert(TestItem).values(
                code=code, name=name, price=price, category=category
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["code"],
                set_={"name": name, "price": price, "category": category},
            )

            # 1. Execute statement with RETURNING
            result = self.session.scalars(
                select(TestItem).from_statement(stmt.returning(TestItem))
            ).first()

            # 2. Expire session cache so ORM re-syncs all attributes from RETURNING payload
            self.session.expire_all()

            # 3. Return re-synced item
            return self.session.scalars(
                select(TestItem).where(TestItem.code == code)
            ).first()

        elif self.dialect_name in ("mariadb", "mysql"):
            stmt = mariadb_insert(TestItem).values(
                code=code, name=name, price=price, category=category
            )
            stmt = stmt.on_duplicate_key_update(
                name=name, price=price, category=category
            )
            self.session.execute(stmt)
            self.session.flush()

            # Evict existing stale instance from identity map
            self.session.expire_all()

            return self.session.scalars(
                select(TestItem).where(TestItem.code == code)
            ).first()

        else:
            raise NotImplementedError(
                f"UPSERT not implemented for dialect: {self.dialect_name}"
            )

    def search_by_code_regex_custom_query(self, pattern: str) -> Sequence[TestItem]:
        """Custom SQL query using dialect-specific Regular Expression matching."""
        if self.dialect_name == "postgresql":
            # PostgreSQL case-insensitive regex operator '~*'
            raw_sql = text("SELECT * FROM test.test_items WHERE code ~* :pat")
        elif self.dialect_name in ("mariadb", "mysql"):
            # MariaDB REGEXP operator
            raw_sql = text("SELECT * FROM test.test_items WHERE code REGEXP :pat")
        else:
            raise NotImplementedError(
                f"Regex query not supported for dialect {self.dialect_name}"
            )

        return self.session.scalars(
            select(TestItem).from_statement(raw_sql), params={"pat": pattern}
        ).all()

    def get_most_expensive_item_per_category(self) -> Sequence[TestItem]:
        """Custom query using SQLAlchemy Window Functions (ROW_NUMBER OVER PARTITION BY)."""
        rn = (
            func.row_number()
            .over(partition_by=TestItem.category, order_by=TestItem.price.desc())
            .label("rn")
        )

        subquery = (
            select(TestItem.id, rn).where(TestItem.category.is_not(None)).subquery()
        )

        stmt = (
            select(TestItem)
            .join(subquery, TestItem.id == subquery.c.id)
            .where(subquery.c.rn == 1)
        )
        return self.session.scalars(stmt).all()


class ProjectRepository(BaseRepository[TestProject]):
    """Custom repository using aggregation queries and joins."""

    def __init__(self, session):
        super().__init__(model=TestProject, session=session)

    def get_projects_with_task_counts(self) -> Sequence[tuple[TestProject, int]]:
        """Custom JOIN + GROUP BY aggregation query."""
        stmt = (
            select(TestProject, func.count(TestTask.id).label("task_count"))
            .outerjoin(TestProject.tasks)
            .group_by(TestProject.id)
            .order_by(TestProject.name)
        )
        return self.session.execute(stmt).all()


# =====================================================================
# PYTEST FIXTURES
# =====================================================================


@pytest.fixture
def item_repo(db_session):
    return ItemRepository(session=db_session)


@pytest.fixture
def composite_repo(db_session):
    return BaseRepository(model=TestCompositeItem, session=db_session)


@pytest.fixture
def project_repo(db_session):
    return ProjectRepository(session=db_session)


@pytest.fixture
def task_repo(db_session):
    return BaseRepository(model=TestTask, session=db_session)


# =====================================================================
# 1. CUSTOM REPOSITORY QUERY TESTS
# =====================================================================
class TestCustomRepositories:

    def test_upsert_item_insert_and_update(self, item_repo):
        """Tests dialect-specific UPSERT custom query."""
        item1 = item_repo.upsert_item(
            code="E1", name="Original Name", price=10.0, category="Electronics"
        )
        assert item1.name == "Original Name"

        item2 = item_repo.upsert_item(
            code="E1", name="Updated Name", price=15.5, category="Hardware"
        )
        assert item2.name == "Updated Name"
        assert item2.price == 15.5

        assert item_repo.count() == 1

    def test_search_by_code_regex_custom_query(self, item_repo):
        """Tests engine-specific raw SQL query for regex searching."""
        item_repo.create(name="Item A", code="ABC-123", price=5.0)
        item_repo.create(name="Item B", code="XYZ-999", price=5.0)
        item_repo.create(name="Item C", code="abc-456", price=5.0)

        # Match codes starting with ABC/abc
        results = item_repo.search_by_code_regex_custom_query(pattern="^abc")

        assert len(results) == 2
        codes = [item.code for item in results]
        assert "ABC-123" in codes
        assert "abc-456" in codes

    def test_window_function_most_expensive_per_category(self, item_repo):
        """Tests window function query across both database engines."""
        item_repo.create(name="Cheap Drill", code="D1", category="Tools", price=25.0)
        item_repo.create(name="Pro Drill", code="D2", category="Tools", price=120.0)
        item_repo.create(name="Small Rake", code="R1", category="Garden", price=15.0)
        item_repo.create(name="Big Mower", code="M1", category="Garden", price=300.0)

        results = item_repo.get_most_expensive_item_per_category()

        assert len(results) == 2
        most_expensive = {item.category: item.name for item in results}
        assert most_expensive["Tools"] == "Pro Drill"
        assert most_expensive["Garden"] == "Big Mower"

    def test_project_repo_task_counts(self, project_repo, task_repo):
        """Tests aggregate custom query with group by."""
        p1 = project_repo.create(name="Project One")
        p2 = project_repo.create(name="Project Two")

        task_repo.create(title="Task 1.1", project_id=p1.id)
        task_repo.create(title="Task 1.2", project_id=p1.id)

        results = project_repo.get_projects_with_task_counts()

        counts = {p.name: count for p, count in results}
        assert counts["Project One"] == 2
        assert counts["Project Two"] == 0


# =====================================================================
# 2. CREATE & ADD OPERATIONS
# =====================================================================
class TestCreateAndAdd:

    def test_create_generates_id_and_flushes(self, item_repo):
        item = item_repo.create(name="Hammer", code="H1", category="Hardware")
        assert item.id is not None
        assert item.name == "Hammer"
        assert item.is_active is True

    def test_add_instantiated_object(self, item_repo):
        raw_item = TestItem(name="Screwdriver", code="S1")
        added_item = item_repo.add(raw_item)
        assert added_item.id is not None
        assert added_item.code == "S1"

    def test_create_with_duplicate_unique_field_raises_error(
        self, item_repo, db_session
    ):
        item_repo.create(name="Item 1", code="UNIQUE_CODE")

        with pytest.raises((IntegrityError, DBAPIError)):
            with db_session.begin_nested():
                item_repo.create(name="Item 2", code="UNIQUE_CODE")


# =====================================================================
# 3. READ OPERATIONS
# =====================================================================
class TestReadOperations:

    def test_get_by_id_single_pk(self, item_repo):
        item = item_repo.create(name="Drill", code="D1")
        found = item_repo.get_by_id(item.id)
        assert found is not None
        assert found.code == "D1"
        assert item_repo.get_by_id(99999) is None

    def test_get_by_id_composite_pk_tuple(self, composite_repo):
        composite_repo.create(tenant_id=1, item_id=100, title="Doc 1")
        found = composite_repo.get_by_id((1, 100))
        assert found is not None
        assert found.title == "Doc 1"

    def test_get_by_id_composite_pk_dict(self, composite_repo):
        composite_repo.create(tenant_id=2, item_id=200, title="Doc 2")
        found = composite_repo.get_by_id({"tenant_id": 2, "item_id": 200})
        assert found is not None
        assert found.title == "Doc 2"

    def test_get_all_with_pagination(self, item_repo):
        for i in range(10):
            item_repo.create(name=f"Item {i}", code=f"CODE_{i}")

        page_1 = item_repo.get_all(limit=3, offset=0)
        assert len(page_1) == 3
        assert page_1[0].code == "CODE_0"

        page_2 = item_repo.get_all(limit=3, offset=3)
        assert len(page_2) == 3
        assert page_2[0].code == "CODE_3"

    def test_filter_by_multiple_fields(self, item_repo):
        item_repo.create(name="A", code="C1", category="Tools", is_active=True)
        item_repo.create(name="B", code="C2", category="Tools", is_active=False)
        item_repo.create(name="C", code="C3", category="Garden", is_active=True)

        results = item_repo.filter_by(category="Tools", is_active=True)
        assert len(results) == 1
        assert results[0].code == "C1"

    def test_count(self, item_repo):
        item_repo.create(name="A", code="C1", is_active=True)
        item_repo.create(name="B", code="C2", is_active=True)
        item_repo.create(name="C", code="C3", is_active=False)

        assert item_repo.count() == 3
        assert item_repo.count(is_active=True) == 2
        assert item_repo.count(is_active=False) == 1


# =====================================================================
# 4. RELATIONAL OPERATIONS
# =====================================================================
class TestRelationalOperations:

    def test_create_child_using_parent_foreign_key(self, project_repo, task_repo):
        project = project_repo.create(name="Backend Refactor")
        task = task_repo.create(
            title="Write Unit Tests", project_id=project.id, status="IN_PROGRESS"
        )
        assert task.id is not None
        assert task.project_id == project.id

    def test_nested_relationship_creation(self, project_repo):
        project = TestProject(
            name="Frontend Migration",
            tasks=[
                TestTask(title="Setup Vite"),
                TestTask(title="Migrate React Components"),
            ],
        )
        saved_project = project_repo.add(project)
        assert saved_project.id is not None
        assert len(saved_project.tasks) == 2

    def test_filter_and_count_by_foreign_key(self, project_repo, task_repo):
        p1 = project_repo.create(name="Project 1")
        p2 = project_repo.create(name="Project 2")

        task_repo.create(title="Task 1.1", project_id=p1.id)
        task_repo.create(title="Task 1.2", project_id=p1.id)
        task_repo.create(title="Task 2.1", project_id=p2.id)

        assert len(task_repo.filter_by(project_id=p1.id)) == 2
        assert task_repo.count(project_id=p2.id) == 1


# =====================================================================
# 5. DELETE OPERATIONS
# =====================================================================
class TestDeleteOperations:

    def test_delete_by_instance(self, item_repo):
        item = item_repo.create(name="To Delete", code="DEL1")
        item_repo.delete(item)
        assert item_repo.get_by_id(item.id) is None

    def test_delete_by_id_single_pk(self, item_repo):
        item = item_repo.create(name="To Delete ID", code="DEL2")
        item_id = item.id

        success = item_repo.delete_by_id(item_id)
        assert success is True
        assert item_repo.get_by_id(item_id) is None
        assert item_repo.delete_by_id(99999) is False

    def test_delete_by_id_composite_pk(self, composite_repo):
        composite_repo.create(tenant_id=10, item_id=50, title="Comp Delete")
        success = composite_repo.delete_by_id({"tenant_id": 10, "item_id": 50})
        assert success is True
        assert composite_repo.get_by_id((10, 50)) is None

    def test_cascade_delete_removes_children(self, project_repo, task_repo):
        project = project_repo.create(name="API Service")
        t1 = task_repo.create(title="Task 1", project_id=project.id)
        t2 = task_repo.create(title="Task 2", project_id=project.id)

        t1_id, t2_id = t1.id, t2.id
        project_repo.delete(project)

        assert project_repo.get_by_id(project.id) is None
        assert task_repo.get_by_id(t1_id) is None
        assert task_repo.get_by_id(t2_id) is None
