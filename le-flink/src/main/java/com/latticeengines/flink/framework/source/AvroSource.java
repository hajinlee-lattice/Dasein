package com.latticeengines.flink.framework.source;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.Path;

public class AvroSource extends FlinkSource<GenericRecord> {

    public AvroSource(ExecutionEnvironment env, String pathWithFs) {
        super(env, pathWithFs);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected DataSource<GenericRecord> createDataSource(ExecutionEnvironment env, String pathWithFs) {
        AvroInputFormat inputFormat = new AvroInputFormat(new Path(pathWithFs));
        return env.createInput(inputFormat);
    }
}
