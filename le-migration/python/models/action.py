from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import BIGINT

from models.base_model import BaseModel, Base


class Action(BaseModel, Base):
    __tablename__ = 'ACTION'
    pid = Column('PID', BIGINT(20), nullable=False, primary_key=True, autoincrement=True)
    fkTenantId = Column('FK_TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID', ondelete='CASCADE'), nullable=False)
