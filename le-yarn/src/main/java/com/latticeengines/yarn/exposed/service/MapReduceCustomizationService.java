package com.latticeengines.yarn.exposed.service;

import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

public interface MapReduceCustomizationService {
    
    void addCustomizations(Job mrJob, String mrJobType, Properties properties);

    void validate(Job mrJob, Properties properties);

}
