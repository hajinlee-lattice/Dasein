package com.latticeengines.apps.cdl.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;

public interface CDLDataCleanupService {

    ApplicationId cleanupData(String customerSpace, CleanupOperationConfiguration configuration);

    void createCleanupAction(String customerSpace, CleanupOperationConfiguration configuration);
}
