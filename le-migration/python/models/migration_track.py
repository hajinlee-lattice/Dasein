from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, LONGBLOB, JSON

from models.base_model import BaseModel, Base


class MigrationTrack(BaseModel, Base):
    __tablename__ = 'MIGRATION_TRACK'
    pid = Column('PID', BIGINT(20), nullable=False, primary_key=True, autoincrement=True)
    # SCHEDULED -> StARTED -> FAILED (COMPLETED)
    status = Column('STATUS', VARCHAR(45), nullable=False)
    version = Column('VERSION', VARCHAR(45), nullable=False)
    curActiveTable = Column('CUR_ACTIVE_TABLE_NAME', JSON, nullable=True,
                            comment='DataCollectionTable.ROLE -> list of Table.NAME')
    # TODO - after MIGRATION_TRACKING table is in, add ForeignKey
    trackingReport = Column('FK_TRACKING_REPORT', BIGINT(20), nullable=True, comment='MIGRATE_TRACKING.pid')
    collectionStatusDetail = Column('DETAIL', JSON, nullable=True, comment='from DATA_COLLECTION_STATUS.Detail')
    statsCubesData = Column('CUBES_DATA', LONGBLOB, nullable=True, comment='from STATISTICS.CUBES_DATA')
    statsName = Column('NAME', VARCHAR(255), nullable=False, comment='from STATISTICS.NAME')
    fkTenantId = Column('FK_TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID', ondelete="CASCADE"), nullable=False)
    fkCollectionId = Column('FK_COLLECTION_ID', BIGINT(20), ForeignKey('METADATA_DATA_COLLECTION.PID'), nullable=True)
