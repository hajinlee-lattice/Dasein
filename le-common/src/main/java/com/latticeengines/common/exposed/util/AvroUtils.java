package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AvroUtils {

    private static final Log log = LogFactory.getLog(AvroUtils.class);

    public static FileReader<GenericRecord> getAvroFileReader(Configuration config, Path path) {
        SeekableInput input = null;
        FileReader<GenericRecord> reader = null;
        try {
            input = new FsInput(path, config);
            GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<GenericRecord>();
            reader = DataFileReader.openReader(input, fileReader);
        } catch (IOException e) {
            log.error("failed to get the reader");
            e.printStackTrace();
        }
        return reader;
    }

    public static Schema getSchema(Configuration config, Path path) {
        FileReader<GenericRecord> reader = getAvroFileReader(config, path);
        Schema schema = reader.getSchema();
        try {
            reader.close();
        } catch (IOException e) {
            log.error("failed to close the reader");
            e.printStackTrace();
        }
        return schema;
    }

    public static List<GenericRecord> getData(Configuration config, Path path) throws Exception {
        FileReader<GenericRecord> reader = getAvroFileReader(config, path);
        List<GenericRecord> data = new ArrayList<GenericRecord>();

        for (GenericRecord datum : reader) {
            data.add(datum);
        }
        try {
            reader.close();
        } catch (IOException e) {
            log.error("failed to close the reader");
            e.printStackTrace();
        }
        return data;
    }
}
