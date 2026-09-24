from typing import Any, Tuple, Type
import pytest
import redis
from sqlalchemy.orm import Session

from v0_1.infrastructure.adapters.secondary.database.base_cached_sql_repo import (
    BaseCachedSQLRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_redis_repo import (
    BaseRedisRepository,
)
from v0_1.infrastructure.adapters.secondary.database.unit_of_work import UnitOfWork
from tests.v0_1.conftest_models import (
    Order,
    OrderItem,
    OrderORM,
    OrderSQLRepo,
    SimpleItem,
    SimpleItemORM,
    SimpleItemSQLRepo,
    TenantUser,
    TenantUserORM,
    TenantUserSQLRepo,
)


# --- Helper to initialize fresh repository and UoW instances ---
def _build_uow_repo(
    db_session: Session,
    redis_client: redis.Redis,
    repo_cls: Type[Any],
    sql_repo_cls: Type[Any],
    orm_cls: Type[Any],
) -> Tuple[UnitOfWork, BaseCachedSQLRepository]:
    uow = UnitOfWork(session=db_session, redis_client=redis_client)
    sql_repo = sql_repo_cls(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client, domain_cls=repo_cls, orm_cls=orm_cls
    )
    repo = BaseCachedSQLRepository(sql_repo=sql_repo, redis_repo=redis_repo, uow=uow)
    return uow, repo


# --- Shared Test Data Parameters ---
ENTITY_CASES = [
    # Case 1: Basic Object
    pytest.param(
        SimpleItem(id="item-uow-1", name="UoW Item"),
        SimpleItem(id="item-uow-1", name="Edited UoW Item"),
        SimpleItem(id="item-uow-2", name="Rollback Item"),
        "item-uow-1",
        "item-uow-2",
        SimpleItem,
        SimpleItemSQLRepo,
        SimpleItemORM,
        id="basic_item",
    ),
    # Case 2: Relationships
    pytest.param(
        Order(
            id="ord-uow-1",
            customer="Bob",
            items=[OrderItem(id="oi-2", product_name="Laptop", quantity=1)],
        ),
        Order(
            id="ord-uow-1",
            customer="Totally Not Bob",
            items=[OrderItem(id="oi-2", product_name="Laptop", quantity=1)],
        ),
        Order(id="ord-uow-2", customer="Fail", items=[]),
        "ord-uow-1",
        "ord-uow-2",
        Order,
        OrderSQLRepo,
        OrderORM,
        id="order_relationships",
    ),
    # Case 3: Tuple Composite PK
    pytest.param(
        TenantUser(tenant_id="t-200", user_id="u-800", role="Editor"),
        TenantUser(tenant_id="t-200", user_id="u-800", role="Edited Editor"),
        TenantUser(tenant_id="t-999", user_id="u-999", role="Guest"),
        ("t-200", "u-800"),
        ("t-999", "u-999"),
        TenantUser,
        TenantUserSQLRepo,
        TenantUserORM,
        id="tuple_composite_pk",
    ),
    # Case 4: Dict Composite PK
    pytest.param(
        TenantUser(tenant_id="t-200", user_id="u-800", role="Editor"),
        TenantUser(tenant_id="t-200", user_id="u-800", role="Edited Editor"),
        TenantUser(tenant_id="t-999", user_id="u-999", role="Guest"),
        {"tenant_id": "t-200", "user_id": "u-800"},
        {"tenant_id": "t-999", "user_id": "u-999"},
        TenantUser,
        TenantUserSQLRepo,
        TenantUserORM,
        id="dict_composite_pk",
    ),
]


@pytest.mark.parametrize(
    "entity, _edited, _rollback, entity_id, _rb_id, repo_cls, sql_repo_cls, orm_cls",
    ENTITY_CASES,
)
def test_uow_commit_stages_cache_actions_until_commit(
    db_session: Session,
    redis_client: redis.Redis,
    entity,
    _edited,
    _rollback,
    entity_id,
    _rb_id,
    repo_cls,
    sql_repo_cls,
    orm_cls,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, repo_cls, sql_repo_cls, orm_cls
    )

    with uow:
        repo.save(entity)
        # Uncommitted: Cache should still be empty
        assert repo.redis.get(entity_id) is None

    # Post-commit: Cache populated via read-through or commit hooks
    assert repo.get_by_id(entity_id) == entity
    assert repo.redis.get(entity_id) == entity


@pytest.mark.parametrize(
    "entity, _edited, rollback_entity, _id, rollback_id, repo_cls, sql_repo_cls, orm_cls",
    ENTITY_CASES,
)
def test_uow_rollback_discards_staged_cache_actions(
    db_session: Session,
    redis_client: redis.Redis,
    entity,
    _edited,
    rollback_entity,
    _id,
    rollback_id,
    repo_cls,
    sql_repo_cls,
    orm_cls,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, repo_cls, sql_repo_cls, orm_cls
    )

    with pytest.raises(RuntimeError, match="Force Rollback"):
        with uow:
            repo.save(rollback_entity)
            raise RuntimeError("Force Rollback")

    assert repo.sql.get_by_id(rollback_id) is None
    assert repo.redis.get(rollback_id) is None


@pytest.mark.parametrize(
    "entity, _edited, _rollback, entity_id, _rb_id, repo_cls, sql_repo_cls, orm_cls",
    ENTITY_CASES,
)
def test_uow_deletion_evicts_cache_post_commit(
    db_session: Session,
    redis_client: redis.Redis,
    entity,
    _edited,
    _rollback,
    entity_id,
    _rb_id,
    repo_cls,
    sql_repo_cls,
    orm_cls,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, repo_cls, sql_repo_cls, orm_cls
    )

    # Setup persisted state
    with uow:
        repo.save(entity)

    with uow:
        repo.delete(entity)
        # Uncommitted delete: Key still present in cache
        assert repo.redis.get(entity_id) is not None

    # Post-commit delete: Key evicted from both cache and database
    assert repo.redis.get(entity_id) is None
    assert repo.sql.get_by_id(entity_id) is None


@pytest.mark.parametrize(
    "entity, edited_entity, _rb, _id, _rb_id, repo_cls, sql_repo_cls, orm_cls",
    ENTITY_CASES,
)
def test_uow_list_all_and_edit_invalidates_collection_cache(
    db_session: Session,
    redis_client: redis.Redis,
    entity,
    edited_entity,
    _rb,
    _id,
    _rb_id,
    repo_cls,
    sql_repo_cls,
    orm_cls,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, repo_cls, sql_repo_cls, orm_cls
    )

    with uow:
        repo.save(entity)
        elements, _ = repo.list_all()
        assert len(elements) > 0
        repo.save(edited_entity)

    # Editing invalidates related collection caches
    cached_elements, _ = repo.redis.list_all()
    assert len(cached_elements) == 0


@pytest.mark.parametrize(
    "entity, _edited, _rb, _id, _rb_id, repo_cls, sql_repo_cls, orm_cls",
    ENTITY_CASES,
)
def test_uow_list_all_and_delete_invalidates_collection_cache(
    db_session: Session,
    redis_client: redis.Redis,
    entity,
    _edited,
    _rb,
    _id,
    _rb_id,
    repo_cls,
    sql_repo_cls,
    orm_cls,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, repo_cls, sql_repo_cls, orm_cls
    )

    with uow:
        repo.save(entity)
        elements, _ = repo.list_all()
        assert len(elements) > 0
        repo.delete(entity)

    # Deleting invalidates related collection caches
    cached_elements, _ = repo.redis.list_all()
    assert len(cached_elements) == 0


def test_uow_handles_redis_pipeline_failure_gracefully(
    db_session: Session,
    redis_client: redis.Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, SimpleItem, SimpleItemSQLRepo, SimpleItemORM
    )
    item = SimpleItem(id="item-fail-1", name="Fail Test")

    # Simulate Redis connection failure on pipeline execute
    def mock_execute(*args, **kwargs):
        raise redis.RedisError("Redis connection lost post-commit")

    with uow:
        repo.save(item)
        monkeypatch.setattr(uow.pipeline, "execute", mock_execute)

    # DB record exists despite cache failure
    assert repo.sql.get_by_id("item-fail-1") is not None


def test_uow_multiple_saves_in_single_transaction(
    db_session: Session,
    redis_client: redis.Redis,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, SimpleItem, SimpleItemSQLRepo, SimpleItemORM
    )
    item1 = SimpleItem(id="item-m-1", name="Item 1")
    item2 = SimpleItem(id="item-m-2", name="Item 2")

    with uow:
        repo.save(item1)
        repo.save(item2)

    # Post-commit: Both items cached and retrievable
    assert repo.redis.get("item-m-1") == item1
    assert repo.redis.get("item-m-2") == item2


def test_uow_get_inside_transaction_does_not_pollute_redis(
    db_session: Session,
    redis_client: redis.Redis,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, SimpleItem, SimpleItemSQLRepo, SimpleItemORM
    )
    item = SimpleItem(id="item-read-1", name="Uncommitted Read")

    with uow:
        repo.save(item)
        # Read inside transaction pulls from DB session
        fetched = repo.get_by_id("item-read-1")
        assert fetched == item
        # Redis MUST remain empty until commit!
        assert repo.redis.get("item-read-1") is None

    # After commit, Redis is populated
    assert repo.redis.get("item-read-1") == item


def test_uow_pipeline_isolation_across_sequential_uses(
    db_session: Session,
    redis_client: redis.Redis,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, SimpleItem, SimpleItemSQLRepo, SimpleItemORM
    )
    item1 = SimpleItem(id="seq-1", name="First Batch")
    item2 = SimpleItem(id="seq-2", name="Second Batch")

    # Transaction 1
    with uow:
        repo.save(item1)

    # Transaction 2 using same UoW instance
    with uow:
        repo.save(item2)

    # Check pipeline and staging queues are clear and independent
    assert len(uow._staged_cache_actions) == 0
    assert repo.redis.get("seq-1") == item1
    assert repo.redis.get("seq-2") == item2


def test_uow_save_invalidates_paginated_pattern_keys(
    db_session: Session,
    redis_client: redis.Redis,
) -> None:
    uow, repo = _build_uow_repo(
        db_session, redis_client, SimpleItem, SimpleItemSQLRepo, SimpleItemORM
    )
    item = SimpleItem(id="item-pg-1", name="Page Invalidation")

    # Manually populate paginated cache keys
    redis_client.set("page:1:size:5:simpleitem", "cached_page_data")
    redis_client.set("page:2:size:5:simpleitem", "cached_page_data")

    with uow:
        repo.save(item)

    # Lua / pattern deletion should have purged paginated keys post-commit
    assert redis_client.get("simpleitem:page:1") is None
    assert redis_client.get("simpleitem:page:2") is None
