import json
from typing import Generator, List
import pytest
import redis
from sqlalchemy import ARRAY, Integer, String, cast, func, select, type_coerce
from sqlalchemy.orm import Session

from tests.v0_1.conftest_models import SimpleItem, SimpleItemORM, SimpleItemSQLRepo
from v0_1.infrastructure.adapters.secondary.database.base_cached_sql_repo import (
    BaseCachedSQLRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_redis_repo import (
    BaseRedisRepository,
)
from v0_1.infrastructure.adapters.secondary.database.unit_of_work import (
    UnitOfWork,
)


@pytest.fixture
def seed_items(db_session: Session) -> Generator[List[SimpleItem], None, None]:
    """Seeds 15 SimpleItem records into the DB for pagination testing."""
    items = [
        SimpleItemORM(id=f"item-{i:02d}", name=f"Item {i:02d}") for i in range(1, 18)
    ]
    db_session.add_all(items)
    db_session.commit()

    domain_items = [SimpleItem(id=item.id, name=item.name) for item in items]
    yield domain_items

    db_session.query(SimpleItemORM).delete()
    db_session.commit()


def test_sql_list_all(db_session: Session, seed_items: List[SimpleItem]) -> None:
    sql_repo = SimpleItemSQLRepo(session=db_session)

    all_list, total = sql_repo.list_all()
    assert total == 17
    for i in range(0, len(all_list)):
        assert all_list[i].id == f"item-{(i+1):02d}"


def test_sql_list_paginated_offset(
    db_session: Session, seed_items: List[SimpleItem]
) -> None:
    """Tests offset-based pagination directly on the SQL repository."""
    sql_repo = SimpleItemSQLRepo(session=db_session)

    # Page 1 (items 1-5 out of 15)
    page_1, total = sql_repo.list_all_paginated_offset(page=1, page_size=5)
    assert total == 17
    assert len(page_1) == 5
    assert page_1[0].id == "item-01"
    assert page_1[4].id == "item-05"

    # Page 2 (items 6-10 out of 15)
    page_2, total = sql_repo.list_all_paginated_offset(page=2, page_size=5)
    assert total == 17
    assert len(page_2) == 5
    assert page_2[0].id == "item-06"
    assert page_2[4].id == "item-10"

    # Page 4 (last page)
    page_4, total = sql_repo.list_all_paginated_offset(page=4, page_size=5)
    assert total == 17
    assert len(page_4) == 2
    assert page_4[0].id == "item-16"
    assert page_4[1].id == "item-17"

    # Page 5 (out-of-bounds slice -> empty list)
    page_5, total = sql_repo.list_all_paginated_offset(page=5, page_size=5)
    assert total == 17
    assert len(page_5) == 0


def test_sql_list_all_filtered(
    db_session: Session, seed_items: List[SimpleItem]
) -> None:
    sql_repo = SimpleItemSQLRepo(session=db_session)

    # Filter which returns no values
    stmt = select(SimpleItemORM).where(func.char_length(SimpleItemORM.name) > 10000)
    all_list, total = sql_repo.list_all_filtered(stmt=stmt)
    assert total == 0

    # Filter which returns all values
    stmt = select(SimpleItemORM).where(func.char_length(SimpleItemORM.name) > 0)
    all_list, total = sql_repo.list_all_filtered(stmt=stmt)
    assert total == 17
    for i in range(0, len(all_list)):
        assert all_list[i].id == f"item-{(i+1):02d}"

    # 2. Filter out rows that have no numeric sequence first
    all_list, total = sql_repo.get_lower_than_id(simpleitem_id=10)
    assert total == 10


def test_sql_list_paginated_filtered(
    db_session: Session, seed_items: List[SimpleItem]
) -> None:
    sql_repo = SimpleItemSQLRepo(session=db_session)

    all_list, total = sql_repo.get_lower_than_id(simpleitem_id=12, page=1, page_size=5)
    assert len(all_list) == 5
    assert total == 12

    all_list, total = sql_repo.get_lower_than_id(simpleitem_id=12, page=2, page_size=5)
    assert len(all_list) == 5
    assert total == 12

    all_list, total = sql_repo.get_lower_than_id(simpleitem_id=12, page=3, page_size=5)
    assert len(all_list) == 2
    assert total == 12


def test_sql_cached_list_all(
    db_session: Session, redis_client: redis.Redis, seed_items: List[SimpleItem]
):
    sql_repo = SimpleItemSQLRepo(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client,
        domain_cls=SimpleItem,
        orm_cls=SimpleItemORM,
    )
    cached_repo = BaseCachedSQLRepository(redis_repo=redis_repo, sql_repo=sql_repo)

    elements, total = cached_repo.list_all()
    assert total == 17
    assert len(elements) == 17

    cached_elements, cached_total = redis_repo.list_all()

    assert cached_elements == elements
    assert cached_total == total


def test_sql_cached_list_all_cache_clear_on_edit(
    db_session: Session, redis_client: redis.Redis, seed_items: List[SimpleItem]
):
    sql_repo = SimpleItemSQLRepo(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client,
        domain_cls=SimpleItem,
        orm_cls=SimpleItemORM,
    )
    cached_repo = BaseCachedSQLRepository(redis_repo=redis_repo, sql_repo=sql_repo)

    elements, total = cached_repo.list_all()
    assert total == 17
    assert len(elements) == 17

    cached_elements, cached_total = redis_repo.list_all()

    assert cached_elements == elements
    assert cached_total == total

    element = seed_items[0]
    element.name = "changed_name"
    cached_repo.save(element)

    cached_elements, cached_total = redis_repo.list_all()
    assert len(cached_elements) == 0
    assert cached_total == 0


def test_sql_cached_list_all_cache_clear_on_delete(
    db_session: Session, redis_client: redis.Redis, seed_items: List[SimpleItem]
):
    sql_repo = SimpleItemSQLRepo(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client,
        domain_cls=SimpleItem,
        orm_cls=SimpleItemORM,
    )
    cached_repo = BaseCachedSQLRepository(redis_repo=redis_repo, sql_repo=sql_repo)

    elements, total = cached_repo.list_all()
    assert total == 17
    assert len(elements) == 17

    cached_elements, cached_total = redis_repo.list_all()

    element = seed_items[0]
    cached_repo.delete(element)

    cached_elements, cached_total = redis_repo.list_all()
    assert len(cached_elements) == 0
    assert cached_total == 0

    elements, total = cached_repo.list_all()
    assert total == 16
    assert len(elements) == 16

    cached_elements, cached_total = redis_repo.list_all()
    assert cached_elements == elements
    assert cached_total == total


def test_sql_cached_list_all_paginated_offset(
    db_session: Session, redis_client, seed_items: List[SimpleItem]
) -> None:
    sql_repo = SimpleItemSQLRepo(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client,
        domain_cls=SimpleItem,
        orm_cls=SimpleItemORM,
    )

    cached_repo = BaseCachedSQLRepository(redis_repo=redis_repo, sql_repo=sql_repo)

    elements, total = cached_repo.list_paginated_offset(page=1, page_size=5)
    assert len(elements) == 5
    assert total == len(seed_items)

    cached_elements, cached_total = redis_repo.list_page(page=1, page_size=5)
    assert cached_elements == elements
    assert cached_total == total

    cached_elements, cached_total = redis_repo._get_list("page:1:size:5:simpleitem")
    assert cached_elements == elements
    assert cached_total == total


def test_sql_cached_list_all_filtered(
    db_session: Session, redis_client, seed_items: List[SimpleItem]
) -> None:
    sql_repo = SimpleItemSQLRepo(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client,
        prefix="simpleitem_idlt",
        domain_cls=SimpleItem,
        orm_cls=SimpleItemORM,
    )

    cached_repo = BaseCachedSQLRepository(redis_repo=redis_repo, sql_repo=sql_repo)

    elements, total = cached_repo.list_all_filtered(
        sql_repo.get_lower_than_id, simpleitem_id=10
    )
    assert len(elements) == 10
    assert total == 10

    cached_elements, cached_total = redis_repo.list_all()
    assert cached_elements == elements
    assert cached_total == total

    cached_elements, cached_total = redis_repo._get_list(
        "all:simpleitem_idlt:simpleitem_id:10"
    )
    assert cached_elements == elements
    assert cached_total == total


def test_sql_cached_list_paginated_filtered_offset(
    db_session: Session, redis_client, seed_items: List[SimpleItem]
) -> None:
    sql_repo = SimpleItemSQLRepo(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client,
        prefix="simpleitem_idlt",
        domain_cls=SimpleItem,
        orm_cls=SimpleItemORM,
    )

    cached_repo = BaseCachedSQLRepository(redis_repo=redis_repo, sql_repo=sql_repo)

    elements, total = cached_repo.list_paginated_filtered_offset(
        sql_repo.get_lower_than_id, page=1, page_size=5, simpleitem_id=8
    )
    assert len(elements) == 5
    assert total == 8

    cached_elements, cached_total = redis_repo.list_page(page=1, page_size=5)

    assert cached_elements == elements
    assert cached_total == total

    cached_elements, cached_total = redis_repo._get_list(
        "page:1:size:5:simpleitem_idlt:simpleitem_id:8"
    )

    assert elements == cached_elements
    assert total == cached_total

    elements, total = cached_repo.list_paginated_filtered_offset(
        sql_repo.get_lower_than_id, page=2, page_size=5, simpleitem_id=8
    )

    assert len(elements) == 3
    assert total == 8

    cached_elements, cached_total = redis_repo.list_page(page=2, page_size=5)

    assert cached_elements == elements
    assert cached_total == total

    cached_elements, cached_total = redis_repo._get_list(
        "page:2:size:5:simpleitem_idlt:simpleitem_id:8"
    )

    assert elements == cached_elements
    assert total == cached_total
