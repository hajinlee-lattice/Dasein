package com.latticeengines.dataplatform.mapreduce.job;

import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

public interface MRJobCustomization {

    void customize(Job mrJob, Properties properties);
}
