package com.latticeengines.domain.exposed.camille.lifecycle;

import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;

public class ServiceInfo {
    public ServiceInfo(ServiceProperties properties, CustomerSpaceServiceInstaller cssInstaller,
            CustomerSpaceServiceUpgrader cssUpgrader, ServiceInstaller installer) {
        this.properties = properties;
        this.cssInstaller = cssInstaller;
        this.cssUpgrader = cssUpgrader;
        this.installer = installer;
    }

    public ServiceProperties properties;
    public CustomerSpaceServiceInstaller cssInstaller;
    public CustomerSpaceServiceUpgrader cssUpgrader;
    public ServiceInstaller installer;
}
