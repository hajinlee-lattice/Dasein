package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface MetadataProvisioningService {

    void provisionImportTables(CustomerSpace customerSpace);
}
