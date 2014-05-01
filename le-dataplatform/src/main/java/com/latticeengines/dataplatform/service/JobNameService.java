package com.latticeengines.dataplatform.service;

import org.joda.time.DateTime;

public interface JobNameService {

    String createJobName(String customer, String jobType);
    
    String getDateTimeStringFromJobName(String jobName);
    
    DateTime getDateTimeFromJobName(String jobName);
    
    String getCustomerFromJobName(String jobName);
}