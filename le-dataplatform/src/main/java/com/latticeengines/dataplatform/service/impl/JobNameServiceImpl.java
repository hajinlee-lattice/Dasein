package com.latticeengines.dataplatform.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Component("jobNameService")
public class JobNameServiceImpl implements JobNameService {

    public static final char JOBNAME_DELIMITER = '~';
    private Joiner joiner = Joiner.on(JOBNAME_DELIMITER);
    private Splitter jobNameSplitter = Splitter.on(JOBNAME_DELIMITER);

    @Override
    public String createJobName(String customer, String jobType) {
        CustomerSpace customerSpace = CustomerSpace.parse(customer);
        String shortName = !customer.equals(customerSpace.toString()) ? customer : customerSpace.getTenantId();
        return joiner.join(shortName, jobType);
    }

    @Override
    public String getCustomerFromJobName(String jobName) {
        List<String> splits = jobNameSplitter.splitToList(jobName);
        return splits.get(0);
    }
}
