package com.latticeengines.yarn.exposed.service;

public interface JobNameService {

    String createJobName(String customer, String jobType);
    String getCustomerFromJobName(String jobName);

}
