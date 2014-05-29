package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.List;

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

    public static FileReader<GenericRecord> getAvroFileReader(Configuration config, Path path) throws Exception {
        SeekableInput input = new FsInput(path, config);
        GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<GenericRecord>();
        return DataFileReader.openReader(input, fileReader);
    }
    public static Schema getSchema(Configuration config, Path path) throws Exception {
        FileReader<GenericRecord> reader = getAvroFileReader(config, path);
        return reader.getSchema();
    }
    
    public static List<GenericRecord> getData(Configuration config, Path path) throws Exception {
        FileReader<GenericRecord> reader = getAvroFileReader(config, path);
        List<GenericRecord> data = new ArrayList<GenericRecord>();
        
        for (GenericRecord datum : reader) {
            data.add(datum);
        }
        return data;
    }
}
