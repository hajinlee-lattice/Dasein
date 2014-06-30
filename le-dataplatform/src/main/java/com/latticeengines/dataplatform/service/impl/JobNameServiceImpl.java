package com.latticeengines.dataplatform.service.impl;

import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.latticeengines.dataplatform.service.JobNameService;

@Component("jobNameService")
public class JobNameServiceImpl implements JobNameService {

    // Is it possible for Customer to include this character?
    private static final char JOBNAME_DELIMITER = '~';
    
    private DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime();
    private Joiner joiner = Joiner.on(JOBNAME_DELIMITER);
    private Splitter jobNameSplitter = Splitter.on(JOBNAME_DELIMITER);
    
    @Override
    public String createJobName(String customer, String jobType) {
        return joiner.join(customer, jobType, dateTimeFormatter.print(new DateTime()));
    }
    
    @Override
    public String getDateTimeStringFromJobName(String jobName) {
        List<String> splits = jobNameSplitter.splitToList(jobName);
        return splits.get(splits.size()-1);
    }
    
    @Override
    public DateTime getDateTimeFromJobName(String jobName) {
        return dateTimeFormatter.parseDateTime(getDateTimeStringFromJobName(jobName));
    }
    
    @Override
    public String getCustomerFromJobName(String jobName) {
        List<String> splits = jobNameSplitter.splitToList(jobName);
        return splits.get(0);
    }
}
