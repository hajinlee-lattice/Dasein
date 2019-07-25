from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT
from sqlalchemy.orm import relationship

from models.base_model import BaseModel, Base
from models.metadata_data_collection_status import MetadataDataCollectionStatus
from models.metadata_data_collection_table import MetadataDataCollectionTable
from models.migration_track import MigrationTrack


class MetadataDataCollection(BaseModel, Base):
    __tablename__ = 'METADATA_DATA_COLLECTION'
    pid = Column('PID', BIGINT(20), nullable=False, primary_key=True, autoincrement=True)
    dataCloudBuildNumber = Column('DATA_CLOUD_BUILD_NUMBER', VARCHAR(255), nullable=True)
    name = Column('NAME', VARCHAR(255), nullable=False)
    tenantId = Column('TENANT_ID', BIGINT(20), nullable=False)
    version = Column('VERSION', VARCHAR(255), nullable=False)
    fkTenantId = Column('FK_TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID'), nullable=False)
    metadataDataCollectionStatus = relationship(
        MetadataDataCollectionStatus, backref='metadataDataCollection', cascade='delete'
    )
    metadataDataCollectionTable = relationship(
        MetadataDataCollectionTable, backref='metadataDataCollection', cascade='delete'
    )
    migrationTrack = relationship(
        MigrationTrack, backref='metadataDataCollection', cascade='delete'
    )
