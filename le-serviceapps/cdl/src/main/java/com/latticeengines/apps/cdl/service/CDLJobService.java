package com.latticeengines.apps.cdl.service;

import java.util.Date;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobService {

    boolean submitJob(CDLJobType cdlJobType, String jobArguments);

    Date getNextInvokeTime(CustomerSpace customerSpace);

    /**
     * Use given scheduler to schedule PAs for all tenants.
     *
     * @param schedulerName
     *            target scheduler name
     * @param dryRun
     *            true if no PA will actually be submitted, only execute scheduling
     *            logic
     */
    void schedulePAJob(@NotNull String schedulerName, boolean dryRun);
}
