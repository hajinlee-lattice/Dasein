from sqlalchemy import Column
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TINYINT
from sqlalchemy.orm import relationship

from models.base_model import BaseModel, Base
from models.import_migrate_tracking import ImportMigrateTracking
from models.metadata_data_collection import MetadataDataCollection
from models.metadata_data_collection_status import MetadataDataCollectionStatus
from models.metadata_data_collection_table import MetadataDataCollectionTable
from models.metadata_statistics import MetadataStatistics
from models.metadata_table import MetadataTable
from models.migration_track import MigrationTrack
from models.action import Action


class Tenant(BaseModel, Base):
    __tablename__ = 'TENANT'
    tenantPid = Column('TENANT_PID', BIGINT(20), nullable=False, primary_key=True)
    contract = Column('CONTRACT', VARCHAR(255), nullable=True)
    externalUserEmailSent = Column('EXTERNAL_USER_EMAIL_SENT', TINYINT(1), nullable=True)
    expiredTime = Column('EXPIRED_TIME', BIGINT(20), nullable=True)
    tenantId = Column('TENANT_ID', VARCHAR(255), nullable=False)
    name = Column('NAME', VARCHAR(255), nullable=False)
    notificationLevel = Column('NOTIFICATION_LEVEL', VARCHAR(20), nullable=True, default='ERROR')
    registeredTime = Column('REGISTERED_TIME', BIGINT(20), nullable=False)
    status = Column('STATUS', VARCHAR(255), nullable=False)
    tenantType = Column('TENANT_TYPE', VARCHAR(255), nullable=False)
    uiVersion = Column('UI_VERSION', VARCHAR(255), nullable=False)
    metadataDataCollection = relationship(MetadataDataCollection, backref='tenant', cascade='delete')
    metadataDataCollectionStatus = relationship(MetadataDataCollectionStatus, backref='tenant', cascade='delete')
    metadataDataCollectionTable = relationship(MetadataDataCollectionTable, backref='tenant', cascade='delete')
    metadataTable = relationship(MetadataTable, backref='tenant', cascade='delete')
    metadataStatistics = relationship(MetadataStatistics, backref='tenant', cascade='delete')
    migrationTrack = relationship(MigrationTrack, cascade='delete', uselist=False, backref='tenant')
    importMigrateTracking = relationship(ImportMigrateTracking, cascade='delete', backref='tenant')
    actions = relationship(Action, cascade='delete', backref='tenant')

    @property
    def activeMetadataStatistics(self):
        version = self.metadataDataCollection[0].version
        return [record for record in self.metadataStatistics if record.version == version]
