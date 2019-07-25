from sqlalchemy.ext.declarative import declarative_base

import models

Base = declarative_base()


class BaseModel:
    """
    Define basic functionality of a model
    """

    def __init__(self, **kwargs):
        if kwargs:
            for k, v in kwargs.items():
                self.__setattr__(k, v)

    def save(self):
        """
            save object to db
        """
        models.storage.new(self)
        models.storage.save()

    def delete(self):
        """
        delete current object
        :return: json repr of object
        """
        o = self.to_json()
        models.storage.delete(self)
        models.storage.save()
        return o

    def to_json(self):
        """
            returns json representation of self
        """
        ignore = ['_sa_instance_state', 'statsCubesData', 'statsData']
        return {k: v for k, v in self.__dict__.items() if k not in ignore}

    def __str__(self):
        return '\n\n'.join(['{}:\n{}'.format(k, v) for k, v in self.to_json().items()])
