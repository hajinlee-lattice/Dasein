import os
import shutil
import boto3
from boto3.s3.transfer import S3Transfer

from .dockerrun import DockerRun
from .ebextension import Extension

S3_BUCKET= 'yintaosong'

class SourceBundle:
    def __init__(self, name):
        self._extensions = []
        self._name = name
        self._dockerrun = None
        pass

    def set_dockerrun(self, dockerrun):
        assert isinstance(dockerrun, DockerRun)
        self._dockerrun = dockerrun
        return self

    def add_extension(self, extension):
        assert isinstance(extension, Extension)
        self._extensions.append(extension)
        return self

    def archive(self):
        tmp_dir = '/tmp/' + self._name
        shutil.rmtree(tmp_dir, ignore_errors=True)
        os.mkdir(tmp_dir)

        if self._dockerrun is not None:
            with open(os.path.join(tmp_dir, DockerRun.filename()), 'w+') as f:
                f.write(self._dockerrun.json())

        if len(self._extensions) > 0:
            os.mkdir(os.path.join(tmp_dir, ".ebextensions"))
            for extension in self._extensions:
                with open(os.path.join(tmp_dir, ".ebextensions", extension.filename()), 'w+') as f:
                    f.write(extension.content())

        shutil.make_archive(self._name, "zip", root_dir=tmp_dir, base_dir=".", verbose=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return self._name + ".zip"

    def upload(self, prefix):
        zip_file = self.archive()
        print 'uploading source bundle to %s' % (os.path.join("https://s3.amazonaws.com", S3_BUCKET, prefix, zip_file) + ' ..')
        client = boto3.client('s3')
        transfer = S3Transfer(client)
        transfer.upload_file(zip_file, S3_BUCKET, os.path.join(prefix, zip_file))
        os.remove(zip_file)
        print 'done.'
