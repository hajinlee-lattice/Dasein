package com.latticeengines.apps.cdl.service;


import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobService {

    boolean submitJob(CDLJobType cdlJobType, String jobArguments);

    ApplicationId createConsolidateJob(String customerSpace);

    ApplicationId createProfileJob(String customerSpace);

}
