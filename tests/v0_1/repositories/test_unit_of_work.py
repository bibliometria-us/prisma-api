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
    "entity, entity_id, repo_cls, sql_repo_cls, orm_cls",
    [
        # Case 1: Basic Object
        (
            SimpleItem(id="item-uow-1", name="UoW Item"),
            "item-uow-1",
            SimpleItem,
            SimpleItemSQLRepo,
            SimpleItemORM,
        ),
        # Case 2: Relationships
        (
            Order(
                id="ord-uow-1",
                customer="Bob",
                items=[OrderItem(id="oi-2", product_name="Laptop", quantity=1)],
            ),
            "ord-uow-1",
            Order,
            OrderSQLRepo,
            OrderORM,
        ),
        # Case 3: Tuple Composite PK
        (
            TenantUser(tenant_id="t-200", user_id="u-800", role="Editor"),
            ("t-200", "u-800"),
            TenantUser,
            TenantUserSQLRepo,
            TenantUserORM,
        ),
        # Case 4: Dict Composite PK
        (
            TenantUser(tenant_id="t-200", user_id="u-800", role="Editor"),
            {"tenant_id": "t-200", "user_id": "u-800"},
            TenantUser,
            TenantUserSQLRepo,
            TenantUserORM,
        ),
    ],
)
def test_unit_of_work_commit_and_rollback_lifecycle(
    db_session: Session,
    redis_client: redis.Redis,
    entity,
    entity_id,
    repo_cls,
    sql_repo_cls,
    orm_cls,
) -> None:
    uow = UnitOfWork(session=db_session, redis_client=redis_client)
    sql_repo = sql_repo_cls(session=db_session)
    redis_repo = BaseRedisRepository(
        redis_client=redis_client, domain_cls=repo_cls, orm_cls=orm_cls
    )

    # Initialize repository WITHOUT get_id_func
    repo = BaseCachedSQLRepository(sql_repo=sql_repo, redis_repo=redis_repo, uow=uow)

    # 1. Commit Scenario: Staged cache actions apply only post-commit
    with uow:
        repo.save(entity)
        assert repo.redis.get(entity_id) is None

    assert repo.get_by_id(entity_id) == entity
    assert repo.redis.get(entity_id) == entity

    # 2. Rollback Scenario: Discards staged actions on exception
    if isinstance(entity, SimpleItem):
        rollback_entity = SimpleItem(id="item-uow-2", name="Rollback")
        rollback_id = "item-uow-2"
    elif isinstance(entity, Order):
        rollback_entity = Order(id="ord-uow-2", customer="Fail", items=[])
        rollback_id = "ord-uow-2"
    elif isinstance(entity_id, dict):
        rollback_entity = TenantUser(tenant_id="t-999", user_id="u-999", role="Guest")
        rollback_id = {"tenant_id": "t-999", "user_id": "u-999"}
    else:
        rollback_entity = TenantUser(tenant_id="t-999", user_id="u-999", role="Guest")
        rollback_id = ("t-999", "u-999")

    with pytest.raises(RuntimeError, match="Force Rollback"):
        with uow:
            repo.save(rollback_entity)
            raise RuntimeError("Force Rollback")

    assert repo.sql.get_by_id(rollback_id) is None
    assert repo.redis.get(rollback_id) is None

    # 3. Deletion Scenario: Staged eviction executes only post-commit
    with uow:
        repo.delete_by_id(entity_id)
        assert repo.redis.get(entity_id) is not None

    assert repo.redis.get(entity_id) is None
    assert repo.sql.get_by_id(entity_id) is None
