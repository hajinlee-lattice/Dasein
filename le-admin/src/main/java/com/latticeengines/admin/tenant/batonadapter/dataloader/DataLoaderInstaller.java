package com.latticeengines.admin.tenant.batonadapter.dataloader;

import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class DataLoaderInstaller extends LatticeComponentInstaller {

    public DataLoaderInstaller() { super(DataLoaderComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        throw new NotImplementedException();
    }

}
