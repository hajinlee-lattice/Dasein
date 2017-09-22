package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobService {

    void submitJob(CDLJobType cdlJobType);

}
