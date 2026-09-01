import pytest
import redis
from sqlalchemy.orm import Session

from v0_1.infrastructure.adapters.secondary.database.base_cached_sql_repo import (
    BaseCachedSQLRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_redis_repo import (
    BaseRedisRepository,
)
from tests.v0_1.conftest_models import (
    SimpleItem,
    SimpleItemORM,
    SimpleItemSQLRepo,
    Order,
    OrderItem,
    OrderORM,
    OrderSQLRepo,
    TenantUser,
    TenantUserORM,
    TenantUserSQLRepo,
)


@pytest.mark.parametrize(
    "entity, entity_id, repo_cls, sql_repo_cls, orm_cls, updated_entity",
    [
        # Case 1: Basic Object
        (
            SimpleItem(id="item-1", name="Basic Item"),
            "item-1",
            SimpleItem,
            SimpleItemSQLRepo,
            SimpleItemORM,
            SimpleItem(id="item-1", name="Updated Basic Item"),
        ),
        # Case 2: Relationships
        (
            Order(
                id="ord-1",
                customer="Alice",
                items=[OrderItem(id="oi-1", product_name="Book", quantity=2)],
            ),
            "ord-1",
            Order,
            OrderSQLRepo,
            OrderORM,
            Order(
                id="ord-1",
                customer="Alice",
                items=[OrderItem(id="oi-1", product_name="Book", quantity=5)],
            ),
        ),
        # Case 3: Tuple Composite Primary Key
        (
            TenantUser(tenant_id="t-100", user_id="u-500", role="Admin"),
            ("t-100", "u-500"),
            TenantUser,
            TenantUserSQLRepo,
            TenantUserORM,
            TenantUser(tenant_id="t-100", user_id="u-500", role="Owner"),
        ),
        # Case 4: Dict Composite Primary Key
        (
            TenantUser(tenant_id="t-100", user_id="u-500", role="Admin"),
            {"tenant_id": "t-100", "user_id": "u-500"},
            TenantUser,
            TenantUserSQLRepo,
            TenantUserORM,
            TenantUser(tenant_id="t-100", user_id="u-500", role="Owner"),
        ),
    ],
)
def test_repository_crud_lifecycle(
    db_session: Session,
    redis_client: redis.Redis,
    entity,
    entity_id,
    repo_cls,
    sql_repo_cls,
    orm_cls,
    updated_entity,
) -> None:
    sql_repo = sql_repo_cls(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client, domain_cls=repo_cls, orm_cls=orm_cls
    )

    # Initialize repository WITHOUT get_id_func
    repo = BaseCachedSQLRepository(sql_repo=sql_repo, redis_repo=redis_repo)

    # 1. Save & Verify Write-Through Caching
    repo.save(entity)
    db_session.flush()

    retrieved = repo.get_by_id(entity_id)
    assert retrieved == entity
    assert repo.redis.get(entity_id) == entity

    # 2. Update & Verify Cache Updates
    repo.save(updated_entity)
    db_session.flush()

    retrieved_updated = repo.get_by_id(entity_id)
    assert retrieved_updated == updated_entity
    assert repo.redis.get(entity_id) == updated_entity

    # 3. Delete & Verify Cache Eviction
    repo.delete_by_id(entity_id)
    db_session.flush()

    assert repo.get_by_id(entity_id) is None
    assert repo.redis.get(entity_id) is None
    assert repo.sql.get_by_id(entity_id) is None
