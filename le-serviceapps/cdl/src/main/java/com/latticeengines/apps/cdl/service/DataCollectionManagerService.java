package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface DataCollectionManagerService {

    boolean resetAll(String customerSpaceStr);

    boolean resetEntity(String customerSpaceStr, BusinessEntity entity);
}
