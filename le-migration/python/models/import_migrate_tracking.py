from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, JSON
from sqlalchemy.orm import relationship

from models.base_model import BaseModel, Base
from models.migration_track import MigrationTrack


class ImportMigrateTracking(BaseModel, Base):
    __tablename__ = 'IMPORT_MIGRATE_TRACKING'
    pid = Column('PID', BIGINT(20), nullable=False, primary_key=True, autoincrement=True)
    report = Column('REPORT', JSON, nullable=True, default=None)
    status = Column('STATUS', VARCHAR(255), nullable=True, default=None)
    fkTenantId = Column('FK_TENANT_ID', BIGINT(20), ForeignKey('TENANT.TENANT_PID', ondelete='CASCADE'), nullable=False)
    migrationTrack = relationship(MigrationTrack, cascade='delete', uselist=False, backref='importMigrateTracking')
