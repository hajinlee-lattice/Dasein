package com.latticeengines.dataplatform.service;

import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

public interface MapReduceCustomizationService {
    
    void addCustomizations(Job mrJob, Properties properties);

    void validate(Job mrJob, Properties properties);

}
