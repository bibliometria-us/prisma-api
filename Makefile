start-celery:
	systemctl start prisma-celery

restart-celery:
	systemctl restart prisma-celery

stop-celery:
	systemctl stop prisma-celery

redeploy-celery:
	cat /var/www/prisma-api/system/prisma-celery.service > /etc/systemd/system/prisma-celery.service
	systemctl daemon-reload
	make restart-celery


start-api:
	systemctl start prisma-api

restart-api:
	systemctl restart prisma-api

stop-api:
	systemctl stop prisma-api

redeploy-api:
	cat /var/www/prisma-api/system/prisma-api.service > /etc/systemd/system/prisma-api.service
	systemctl daemon-reload
	make restart-api