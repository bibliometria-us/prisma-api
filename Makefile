.PHONY: checkall fix lint typecheck securitycheck test \
        start-celery restart-celery stop-celery redeploy-celery \
        start-api restart-api stop-api redeploy-api

# Variables
SRC_DIR = v0_1

start-celery:
	systemctl start prisma-celery

restart-celery:
	systemctl restart prisma-celery

stop-celery:
	systemctl stop prisma-celery

redeploy-celery:
	sudo cp /var/www/prisma-api/system/prisma-celery.service /etc/systemd/system/prisma-celery.service
	sudo systemctl daemon-reload
	$(MAKE) restart-celery

start-api:
	systemctl start prisma-api

restart-api:
	systemctl restart prisma-api

stop-api:
	systemctl stop prisma-api

redeploy-api:
	sudo cp /var/www/prisma-api/system/prisma-api.service /etc/systemd/system/prisma-api.service
	sudo systemctl daemon-reload
	$(MAKE) restart-api

typecheck:
	mypy $(SRC_DIR)

securitycheck:
	bandit -c .bandit.yaml -r ./$(SRC_DIR)

test:
	pytest

lint:
	ruff check $(SRC_DIR)

fix:
	ruff check --fix $(SRC_DIR)

checkall: typecheck securitycheck lint
	@echo "All checks passed successfully!"