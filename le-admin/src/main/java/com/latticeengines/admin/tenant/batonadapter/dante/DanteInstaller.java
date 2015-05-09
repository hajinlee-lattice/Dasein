package com.latticeengines.admin.tenant.batonadapter.dante;


import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

/**
 * This is a dummy installer for functional tests, the true installer resides in Dante project
 */
public class DanteInstaller extends LatticeComponentInstaller {

    public DanteInstaller() { super(DanteComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {}
}
