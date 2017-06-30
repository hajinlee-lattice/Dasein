package com.latticeengines.yarn.exposed.client.mapreduce;

import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

public interface MRJobCustomization {

    String getJobType();
    
    void customize(Job mrJob, Properties properties);
}
