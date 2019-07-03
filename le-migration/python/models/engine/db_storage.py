"""
Define abstracted access to MySQL Database
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from os import getenv
from models.base_model import Base
from models.tenant import Tenant
from models.metadata_data_collection import MetadataDataCollection
from models.metadata_data_collection_status import MetadataDataCollectionStatus
from models.metadata_statistics import MetadataStatistics
from models.metadata_table import MetadataTable
from models.metadata_data_collection_table import MetadataDataCollectionTable
from models.migration_track import MigrationTrack

clsd = {
    'Tenant': Tenant,
    'MetadataDataCollection': MetadataDataCollection,
    'MetadataDataCollectionStatus': MetadataDataCollectionStatus,
    'MetadataDataCollectionTable': MetadataDataCollectionTable,
    'MetadataTable': MetadataTable,
    'MetadataStatistics': MetadataStatistics,
    'MigrationTrack': MigrationTrack
}


class DBStorage:
    __engine = None
    __session = None

    def __init__(self):
        MYSQL_USER = getenv('MYSQL_USER')
        MYSQL_PWD = getenv('MYSQL_PWD')
        MYSQL_HOST = getenv('MYSQL_HOST')
        MYSQL_DB = getenv('MYSQL_DB')
        self.__engine = create_engine('mysql+mysqldb://{}:{}@{}/{}'.
                                      format(MYSQL_USER,
                                             MYSQL_PWD,
                                             MYSQL_HOST,
                                             MYSQL_DB))

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

    def close(self):
        """call remove() method on the private session attribute"""
        self.__session.remove()

