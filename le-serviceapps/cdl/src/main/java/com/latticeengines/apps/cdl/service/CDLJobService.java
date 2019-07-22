package com.latticeengines.apps.cdl.service;

import java.util.Date;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobService {

    boolean submitJob(CDLJobType cdlJobType, String jobArguments);

    Date getNextInvokeTime(CustomerSpace customerSpace);

    void schedulePAJob();
}
