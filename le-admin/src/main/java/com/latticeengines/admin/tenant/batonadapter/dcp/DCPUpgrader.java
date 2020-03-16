package com.latticeengines.admin.tenant.batonadapter.dcp;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

/**
 * This is a dummy upgrader for functional tests, the true destroyer resides on
 * DCP server
 */
public class DCPUpgrader implements CustomerSpaceServiceUpgrader {

    @Override
    public DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion, int targetVersion,
                                     DocumentDirectory source, Map<String, String> properties) {
        return null;
    }
}
