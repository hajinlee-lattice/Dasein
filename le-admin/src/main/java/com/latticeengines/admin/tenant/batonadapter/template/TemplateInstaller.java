package com.latticeengines.admin.tenant.batonadapter.template;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;

public class TemplateInstaller implements CustomerSpaceServiceInstaller {

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties) {
        return null;
    }

    @Override
    public DocumentDirectory getDefaultConfiguration(String serviceName) {
        return null;
    }


}
