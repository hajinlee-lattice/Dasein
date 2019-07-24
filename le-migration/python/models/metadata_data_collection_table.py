from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT

from models.base_model import BaseModel, Base


class MetadataDataCollectionTable(BaseModel, Base):
    __tablename__ = 'METADATA_DATA_COLLECTION_TABLE'
    pid = Column('PID', BIGINT(20), primary_key=True, nullable=False, autoincrement=True)
    role = Column('ROLE', VARCHAR(255), nullable=False)
    tenantId = Column('TENANT_ID', BIGINT(20), nullable=False)
    version = Column('VERSION', VARCHAR(255), nullable=False)
    fkCollectionId = Column('FK_COLLECTION_ID', BIGINT(20), ForeignKey('METADATA_DATA_COLLECTION.PID'), nullable=False)
    fkTableId = Column('FK_TABLE_ID', BIGINT(20), ForeignKey('METADATA_TABLE.PID'), nullable=False)
    fkTenantId = Column('FK_TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID'), nullable=False)
