package com.latticeengines.admin.tenant.batonadapter.vdb;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;

public class VisiDBInstaller implements CustomerSpaceServiceInstaller {

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion) {
        return null;
    }
}
