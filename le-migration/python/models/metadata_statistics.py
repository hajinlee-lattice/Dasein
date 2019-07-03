from models.base_model import BaseModel, Base
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, LONGBLOB


class MetadataStatistics(BaseModel, Base):

    __tablename__ = 'METADATA_STATISTICS'
    pid = Column('PID', BIGINT(20), primary_key=True, nullable=False, autoincrement=True)
    cubesData = Column('CUBES_DATA', LONGBLOB, nullable=True)
    data = Column('DATA', LONGBLOB, nullable=True)
    name = Column('NAME', VARCHAR(255), nullable=False)
    tenantId = Column('TENANT_ID', BIGINT(20), nullable=False)
    version = Column('VERSION', VARCHAR(255), nullable=False)
    fkTenantId = Column('FK_TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID'), nullable=False)
