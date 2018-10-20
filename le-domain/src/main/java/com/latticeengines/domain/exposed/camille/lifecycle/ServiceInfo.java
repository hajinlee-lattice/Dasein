package com.latticeengines.domain.exposed.camille.lifecycle;

import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;

public class ServiceInfo {
    public ServiceProperties properties;
    public CustomerSpaceServiceInstaller cssInstaller;
    public CustomerSpaceServiceUpgrader cssUpgrader;
    public CustomerSpaceServiceDestroyer cssDestroyer;
    public ServiceInstaller installer;
    public ServiceInfo(ServiceProperties properties, CustomerSpaceServiceInstaller cssInstaller,
            CustomerSpaceServiceUpgrader cssUpgrader, CustomerSpaceServiceDestroyer cssDestroyer,
            ServiceInstaller installer) {
        this.properties = properties;
        this.cssInstaller = cssInstaller;
        this.cssUpgrader = cssUpgrader;
        this.cssDestroyer = cssDestroyer;
        this.installer = installer;
    }
}
