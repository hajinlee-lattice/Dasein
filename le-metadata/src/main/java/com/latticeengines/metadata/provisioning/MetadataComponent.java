package com.latticeengines.metadata.provisioning;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;

@Component
public class MetadataComponent {

    @Autowired
    private MetadataComponentManager componentManager;

    public static final String componentName = "Metadata";
    private static final String versionString = "1.0";
    private MetadataInstaller installer = new MetadataInstaller();

    public MetadataComponent() { }

    public static String getVersionString() { return versionString; }

    @PostConstruct
    private void registerBootStrapper() {
        BatonService batonService = new BatonServiceImpl();
        boolean needToRegister = Boolean.valueOf(System.getProperty("com.latticeengines.registerBootstrappers"));
        if (needToRegister && !batonService.getRegisteredServices().contains(componentName)) {
            ServiceProperties serviceProps = new ServiceProperties();
            serviceProps.dataVersion = 1;
            serviceProps.versionString = getVersionString();
            ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
                    getInstaller(), //
                    new MetadataUpgrader(), //
                    null);
            ServiceWarden.registerService(componentName, serviceInfo);
        }
    }

    private CustomerSpaceServiceInstaller getInstaller() {
        installer.setComponentManager(componentManager);
        return installer;
    }
}
