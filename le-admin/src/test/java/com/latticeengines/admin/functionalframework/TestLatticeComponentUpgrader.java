package com.latticeengines.admin.functionalframework;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class TestLatticeComponentUpgrader implements CustomerSpaceServiceUpgrader {

    @Override
    public DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion, int targetVersion,
            DocumentDirectory source, Map<String, String> properties) {
        return null;
    }
}