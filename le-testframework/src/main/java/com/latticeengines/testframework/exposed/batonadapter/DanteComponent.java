package com.latticeengines.testframework.exposed.batonadapter;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;

@Component
public class DanteComponent {

    @Value("${admin.dante.dryrun}")
    private boolean dryrun;

    public static final String componentName = "Dante";
    private static final String versionString = "2.0";

    public DanteComponent() { }

    public static String getVersionString() { return versionString; }

    @PostConstruct
    private void registerBootStrapper() {
        BatonService batonService = new BatonServiceImpl();
        if (dryrun && !batonService.getRegisteredServices().contains(componentName)) {
            ServiceProperties serviceProps = new ServiceProperties();
            serviceProps.dataVersion = 1;
            serviceProps.versionString = getVersionString();
            ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
                    getInstaller(), //
                    new DanteUpgrader(), //
                    getDestroyer(),
                    null);
            ServiceWarden.registerService(componentName, serviceInfo);
        }
    }

    private CustomerSpaceServiceInstaller getInstaller() {
        return new DanteInstaller();
    }

    private CustomerSpaceServiceDestroyer getDestroyer() {
        return new DanteDestroyer();
    }

}
