package com.latticeengines.admin.tenant.batonadapter.template;

import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class TemplateInstaller extends LatticeComponentInstaller {

    public TemplateInstaller() { super(TemplateComponent.componentName); }

    @Override
    public void installCore(
            CustomerSpace space, String serviceName, int dataVersion,
            CustomerSpaceProperties spaceProps, DocumentDirectory autoGenDocDir) {
    }
}
