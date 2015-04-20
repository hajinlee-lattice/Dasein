package com.latticeengines.admin.tenant.batonadapter.template;

import java.util.Map;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TemplateInstaller extends LatticeComponentInstaller {

    public TemplateInstaller() { super(TemplateComponent.componentName); }

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties) {
        return null;
    }
}
