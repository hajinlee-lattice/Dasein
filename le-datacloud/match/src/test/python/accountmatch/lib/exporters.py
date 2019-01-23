import csv
import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import lib.constants as constants


class AvroExporter(object):
    def __init__(self, output_file_path):
        """
        Arguments:
            output_file_path {str} -- path of the output avro file
        """
        self._path = output_file_path
        # build avro schema for test data
        self._schema = avro.schema.parse(
            json.dumps(constants.TEST_AVRO_SCHEMA))

    def __enter__(self):
        # override if the output file already exists
        self._writer = DataFileWriter(
            open(self._path, 'wb'), DatumWriter(), self._schema)

    def __exit__(self, type, value, traceback):
        if self._writer is not None:
            self._writer.close()

    def append(self, rows):
        """append multiple rows of test data to output avro file

        Arguments:
            rows {list(list(str))} -- multiple rows of test data. one row contains a list of column values

        Raises:
            Exception -- if writer is already closed
        """
        if self._writer is None:
            raise Exception('avro writer is not instantiated yet')
        if len(rows) == 0:
            return

        for row in rows:
            obj = dict()
            for i, col in enumerate(constants.TEST_COLS):
                obj[col] = str(row[i]) if row[i] is not None else None
            self._writer.append(obj)


class CsvExporter(object):
    def __init__(self, output_file_path):
        # TODO comments
        self._path = output_file_path

    def __enter__(self):
        if self._path is None:
            return
        # override if the output file already exists
        self._csv_file = open(self._path, 'wb')
        self._csv_writer = csv.writer(self._csv_file, dialect='excel')
        # write csv header for test data
        self._csv_writer.writerow(constants.TEST_COLS)

    def __exit__(self, type, value, traceback):
        if self._csv_file is None:
            return
        self._csv_file.close()

    def append(self, rows):
        """append multiple rows of test data to output csv file

        Arguments:
            rows {list(list(str))} -- multiple rows of test data. one row contains a list of column values

        Raises:
            Exception -- if writer is already closed
        """
        if self._csv_writer is None:
            return
        if len(rows) == 0:
            return

        for row in rows:
            self._csv_writer.writerow(row)
