"""
Define abstracted access to MySQL Database
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.action import Action
from models.base_model import Base
from models.import_migrate_tracking import ImportMigrateTracking
from models.metadata_data_collection import MetadataDataCollection
from models.metadata_data_collection_status import MetadataDataCollectionStatus
from models.metadata_data_collection_table import MetadataDataCollectionTable
from models.metadata_statistics import MetadataStatistics
from models.metadata_table import MetadataTable
from models.migration_track import MigrationTrack
from models.tenant import Tenant

clsd = {
    'Tenant': Tenant,
    'MetadataDataCollection': MetadataDataCollection,
    'MetadataDataCollectionStatus': MetadataDataCollectionStatus,
    'MetadataDataCollectionTable': MetadataDataCollectionTable,
    'MetadataTable': MetadataTable,
    'MetadataStatistics': MetadataStatistics,
    'MigrationTrack': MigrationTrack,
    'ImportMigrateTracking': ImportMigrateTracking,
    'Action': Action
}


class DBStorage:
    __engine = None
    __session = None

    def __init__(self, MYSQL_USER, MYSQL_PWD, MYSQL_HOST, MYSQL_DB):
        self.__engine = create_engine('mysql+mysqldb://{}:{}@{}/{}'.
                                      format(MYSQL_USER,
                                             MYSQL_PWD,
                                             MYSQL_HOST,
                                             MYSQL_DB))
        self.reload()

    def all(self, cls=None):
        if cls is not None and isinstance(cls, str):
            cls = clsd.get(cls)
        return [] if cls is None else self.__session.query(cls).all()

    def new(self, obj=None):
        """add the object to the current database session"""
        if obj is not None:
            self.__session.add(obj)

    def delete(self, obj=None):
        """delete from the current database session obj if not None"""
        if obj is not None:
            self.__session.delete(obj)

    def save(self):
        """commit all changes of the current database session"""
        self.__session.commit()

    def reload(self):
        """reloads data from the database and create new session"""
        Base.metadata.create_all(self.__engine)
        Session = sessionmaker(bind=self.__engine, expire_on_commit=False)
        session = Session()
        self.__session = session

    def rollback(self):
        """abort all changes in current session"""
        self.__session.rollback()
        self.close()

    def close(self):
        """call remove() method on the private session attribute"""
        self.__session.close()

    def getByPid(self, cls=None, pid=None):
        """get an object based on class and pid"""
        if cls is not None and isinstance(cls, str):
            cls = clsd.get(cls)
        return None if cls is None or pid is None else self.__session.query(cls).get(pid)

    def getByColumn(self, cls=None, col=None, val=None):
        """get objects filtered by column value"""
        if cls is not None and isinstance(cls, str):
            cls = clsd.get(cls)
        try:
            return None if cls is None or col is None or val is None else self.__session.query(cls).filter(
                cls.__getattribute__(cls, col) == val).all()
        except AttributeError:
            raise AttributeError('{} doesn not have column {}'.format(cls.__tablename__, col))
