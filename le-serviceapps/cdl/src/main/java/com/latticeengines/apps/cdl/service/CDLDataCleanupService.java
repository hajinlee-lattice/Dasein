package com.latticeengines.apps.cdl.service;

import java.util.Date;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface CDLDataCleanupService {

    ApplicationId cleanupAll(String customerSpace, BusinessEntity entity);

    ApplicationId cleanupAllData(String customerSpace, BusinessEntity entity);

    ApplicationId cleanupByTimeRange(String customerSpace, BusinessEntity entity, Date startTime, Date endTime);

    ApplicationId cleanupByUpload(String customerSpace, BusinessEntity entity, String filePath);
}
