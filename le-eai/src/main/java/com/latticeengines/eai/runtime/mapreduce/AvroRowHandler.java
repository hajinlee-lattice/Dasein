package com.latticeengines.eai.runtime.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

public interface AvroRowHandler {
    
    void startRecord(Record record) throws IOException;

    void handleField(Record record, Field field) throws IOException;
    
    void endRecord(Record record) throws IOException;
}
