package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DropBoxCrossTenantService {

    DropBox create(String customerSpace);

    void delete(String customerSpace);

    String getDropBoxBucket();

    DropBoxSummary getDropBoxSummary(String customerSpace);

    Tenant getDropBoxOwner(String dropBox);

    String getDropBoxPrefix(String customerSpace);
}
