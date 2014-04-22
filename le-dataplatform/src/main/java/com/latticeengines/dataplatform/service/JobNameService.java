package com.latticeengines.dataplatform.service;

import org.joda.time.DateTime;

public interface JobNameService {

    public String createJobName(String customer, String jobType);
    
    public String getDateTimeStringFromJobName(String jobName);
    
    public DateTime getDateTimeFromJobName(String jobName);
    
    public String getCustomerFromJobName(String jobName);
}