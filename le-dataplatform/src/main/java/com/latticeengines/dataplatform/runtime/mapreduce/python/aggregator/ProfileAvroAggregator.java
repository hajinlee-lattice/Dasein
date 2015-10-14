package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.latticeengines.common.exposed.util.AvroUtils;

public class ProfileAvroAggregator extends ProfilingAggregator {

    @Override
    void aggregateToLocal(List<String> localPaths) throws Exception {
        List<GenericRecord> data = appendRecords(localPaths);
        Schema schema = AvroUtils.readSchemaFromLocalFile(localPaths.get(0));
        AvroUtils.writeToLocalFile(schema, data, getName());
    }

    private List<GenericRecord> appendRecords(List<String> localPaths) throws Exception {
        List<GenericRecord> data = new ArrayList<GenericRecord>();
        for (String path : localPaths) {
            data.addAll(AvroUtils.readFromLocalFile(path));
        }
        return data;
    }

    @Override
    public String getName() {
        return FileAggregator.PROFILE_AVRO;
    }

}
