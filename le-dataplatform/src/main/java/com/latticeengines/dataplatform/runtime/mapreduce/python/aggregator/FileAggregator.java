package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

public interface FileAggregator {

    String PROFILE_AVRO = "profile.avro";
    String MODEL_PROFILE_AVRO = "model_profile.avro";
    String DIAGNOSTICS_JSON = "diagnostics.json";
    String MODEL_PICKLE = "model.p";
    String FEATURE_IMPORTANCE_TXT = "rf_model.txt";

    void aggregate(List<String> localPaths, Configuration config, Progressable progressable) throws Exception;

    String getName();
}
