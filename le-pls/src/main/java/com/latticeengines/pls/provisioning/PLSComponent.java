package com.latticeengines.pls.provisioning;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
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
    private PLSDestroyer destroyer = new PLSDestroyer();

    public PLSComponent() { }

    public static String getVersionString() { return versionString; }

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
                        new PLSUpgrader(), //
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
