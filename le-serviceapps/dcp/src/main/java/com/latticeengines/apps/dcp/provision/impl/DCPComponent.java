package com.latticeengines.apps.dcp.provision.impl;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.provision.DCPComponentManager;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;

@Component
public class DCPComponent {

    private final DCPComponentManager componentManager;

    public static final String componentName = "DCP";
    private static final String versionString = "2.0";
    private DCPInstaller installer = new DCPInstaller();
    private DCPDestroyer destroyer = new DCPDestroyer();

    @Inject
    public DCPComponent(DCPComponentManager componentManager) {
        this.componentManager = componentManager;
    }

    private static String getVersionString() { return versionString; }

    @PostConstruct
    private void registerBootStrapper() {
        if (BeanFactoryEnvironment.Environment.WebApp.equals(BeanFactoryEnvironment.getEnvironment())) {
            BatonService batonService = new BatonServiceImpl();
            if (!batonService.getRegisteredServices().contains(componentName)) {
                ServiceProperties serviceProps = new ServiceProperties();
                serviceProps.dataVersion = 1;
                serviceProps.versionString = getVersionString();
                ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
                        getInstaller(), //
                        new DCPUpgrader(), //
                        getDestroyer(),
                        null);
                ServiceWarden.registerService(componentName, serviceInfo);
            }
        }
    }

    private CustomerSpaceServiceInstaller getInstaller() {
        installer.setComponentManager(componentManager);
        return installer;
    }

    private CustomerSpaceServiceDestroyer getDestroyer() {
        destroyer.setComponentManager(componentManager);
        return destroyer;
    }
}
