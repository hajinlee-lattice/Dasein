package com.latticeengines.admin.tenant.batonadapter.template.dl;


import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;


public class DLTemplateInstaller extends LatticeComponentInstaller {

    public DLTemplateInstaller() { super(DLTemplateComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        throw new UnsupportedOperationException();
    }
}
