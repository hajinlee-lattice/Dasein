package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;

public interface FileAggregator {

    public static final String PROFILE_AVRO = "profile.avro";
    public static final String MODEL_PROFILE_AVRO = "model_profile.avro";
    public static final String DIAGNOSTICS_JSON = "diagnostics.json";
    public static final String MODEL_PICKLE = "model.p";
    public static final String FEATURE_IMPORTANCE_TXT = "rf_model.txt";

    @SuppressWarnings("rawtypes")
    void aggregate(List<String> localPaths, Configuration config, Context context) throws Exception;

    String getName();
}
