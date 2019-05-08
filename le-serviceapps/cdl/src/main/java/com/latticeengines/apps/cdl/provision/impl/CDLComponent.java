package com.latticeengines.apps.cdl.provision.impl;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.provision.CDLComponentManager;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;

@Component
public class CDLComponent {

    private final CDLComponentManager componentManager;

    public static final String componentName = "CDL";
    private static final String versionString = "2.0";
    private CDLInstaller installer = new CDLInstaller();
    private CDLDestroyer destroyer = new CDLDestroyer();

    @Inject
    public CDLComponent(CDLComponentManager componentManager) {
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
                        new CDLUpgrader(), //
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
