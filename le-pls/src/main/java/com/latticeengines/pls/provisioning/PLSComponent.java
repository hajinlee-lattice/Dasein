package com.latticeengines.pls.provisioning;

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
public class PLSComponent {

    @Autowired
    private PLSComponentManager componentManager;

    public static final String componentName = "PLS";
    private static final String versionString = "2.0";
    private PLSInstaller installer = new PLSInstaller();

    public PLSComponent() { }

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
                    new PLSUpgrader(), //
                    null);
            ServiceWarden.registerService(componentName, serviceInfo);
        }
    }

    private CustomerSpaceServiceInstaller getInstaller() {
        installer.setComponentManager(componentManager);
        return installer;
    }
}
