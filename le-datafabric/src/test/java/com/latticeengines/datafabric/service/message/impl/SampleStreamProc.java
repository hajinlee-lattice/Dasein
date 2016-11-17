package com.latticeengines.datafabric.service.message.impl;

import org.apache.avro.generic.GenericRecord;

import com.latticeengines.datafabric.service.message.FabricStreamProc;

public class SampleStreamProc implements FabricStreamProc {

    public int messageCount = 0;
    public int invalidMessages = 0;

    public synchronized void processRecord(String recordType, String id, GenericRecord record) {

        if (!id.equals("yyangdev" + messageCount)) {
            invalidMessages++;
        }
        String company = record.get("company").toString();
        if (!company.equals("myCompany")) {
            invalidMessages++;
        }
        messageCount++;
    }
}
