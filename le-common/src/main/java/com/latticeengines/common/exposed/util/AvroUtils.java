package com.latticeengines.common.exposed.util;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AvroUtils {

    public static Schema getSchema(Configuration config, Path path) throws Exception {
        SeekableInput input = new FsInput(path, config);
        GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<GenericRecord>();
        FileReader<GenericRecord> reader = DataFileReader.openReader(input, fileReader);
        return reader.getSchema();
    }
}
