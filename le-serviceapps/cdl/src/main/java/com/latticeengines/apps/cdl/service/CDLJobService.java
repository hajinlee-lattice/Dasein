package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobService {

    boolean submitJob(CDLJobType cdlJobType, String jobArguments);

}
