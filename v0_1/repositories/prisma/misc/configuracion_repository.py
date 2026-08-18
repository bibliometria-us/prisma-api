from v0_1.repositories.base import BaseRepository
from v0_1.models.prisma.misc.models import AConfiguracion
from sqlalchemy.orm import Session


class ConfiguracionRepository(BaseRepository[AConfiguracion]):
    def __init__(self, session: Session):
        super().__init__(model=AConfiguracion, session=session)
