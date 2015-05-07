package com.latticeengines.admin.tenant.batonadapter.template.visidb;

import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

@SuppressWarnings("restriction")
public class VisiDBTemplateInstaller extends LatticeComponentInstaller {

    public VisiDBTemplateInstaller() { super(VisiDBTemplateComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        throw new NotImplementedException();
    }
}
