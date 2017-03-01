package com.latticeengines.flink.framework.source;

import org.apache.avro.generic.GenericModifiableData;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.Path;

public class AvroSource extends FlinkSource {

    public AvroSource(ExecutionEnvironment env, String pathWithFs) {
        super(env, pathWithFs);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected DataSource<?> createDataSource(ExecutionEnvironment env, String pathWithFs) {
        AvroInputFormat<?> inputFormat = new AvroInputFormat(new Path(pathWithFs), Object.class);
        return env.createInput(inputFormat);
    }
}
