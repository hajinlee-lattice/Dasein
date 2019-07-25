from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, DATETIME, JSON

from models.base_model import BaseModel, Base


class MetadataDataCollectionStatus(BaseModel, Base):
    __tablename__ = 'METADATA_DATA_COLLECTION_STATUS'
    pid = Column('PID', BIGINT(20), nullable=False, primary_key=True, autoincrement=True)
    creationTime = Column('CREATION_TIME', DATETIME, nullable=False)
    detail = Column('Detail', JSON, nullable=True)
    updateTime = Column('UPDATE_TIME', DATETIME, nullable=False)
    version = Column('VERSION', VARCHAR(255), nullable=False)
    fkCollectionId = Column('FK_COLLECTION_ID', BIGINT(20), ForeignKey('METADATA_DATA_COLLECTION.PID'), nullable=False)
    tenantId = Column('TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID'), nullable=False)
