package com.latticeengines.dataplatform.exposed.service;

public interface JobNameService {

    String createJobName(String customer, String jobType);
    
    String getCustomerFromJobName(String jobName);
}