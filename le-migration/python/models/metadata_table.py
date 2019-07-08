from models.base_model import BaseModel, Base
from models.metadata_data_collection_table import MetadataDataCollectionTable
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, BIT, INTEGER
from sqlalchemy.orm import relationship


class MetadataTable(BaseModel, Base):

    __tablename__ = 'METADATA_TABLE'
    pid = Column('PID', BIGINT(20), primary_key=True, nullable=False, autoincrement=True)
    displayName = Column('DISPLAY_NAME', VARCHAR(255), nullable=False)
    interpretation = Column('INTERPRETATION', VARCHAR(255), nullable=True)
    markedForPurge = Column('MARKED_FOR_PURGE', BIT(1), nullable=False)
    name = Column('NAME', VARCHAR(255), nullable=False)
    namespace = Column('NAMESPACE', VARCHAR(255), nullable=True)
    type = Column('TYPE', INTEGER(11), nullable=False)
    tenantId = Column('TENANT_ID', BIGINT(20), nullable=False)
    fkTenantId = Column('FK_TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID'), nullable=False)
    metadataDataCollectionTable = relationship(MetadataDataCollectionTable, backref='metadataTable', cascade='delete')
