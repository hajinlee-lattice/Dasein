package com.latticeengines.pls.provisioning;

import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;

@Component
public class PLSComponent {
    public static final String componentName = "PLS";
    private static final String versionString = "2.0";

    public static String getVersionString() { return versionString; }

    public void registerBootStrapper() {
        ServiceProperties serviceProps = new ServiceProperties();
        serviceProps.dataVersion = 1;
        serviceProps.versionString = getVersionString();
        ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
                getInstaller(), //
                new PLSUpgrader(), //
                null);
        ServiceWarden.registerService(componentName, serviceInfo);
    }

    public PLSComponent() { registerBootStrapper(); }

    private PLSInstaller getInstaller() { return new PLSInstaller(); }
}
