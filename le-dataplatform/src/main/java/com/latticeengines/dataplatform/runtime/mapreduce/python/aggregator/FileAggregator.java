package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

public interface FileAggregator {

    public static final String PROFILE_AVRO = "profile.avro";
    public static final String DIAGNOSTICS_JSON = "diagnostics.json";
    public static final String MODEL_PICKLE = "model.p";

    void aggregate(List<String> localPaths, Configuration config) throws Exception;

    String getName();
}
