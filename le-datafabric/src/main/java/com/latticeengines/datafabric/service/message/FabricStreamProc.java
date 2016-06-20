package com.latticeengines.datafabric.service.message.impl;

import org.apache.avro.generic.GenericRecord;

public interface FabricStreamProc {
    void processRecord(String recordType, String id, GenericRecord record);
}
