package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.InstallerAdaptor;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.ServiceInstallerAdaptor;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServiceBootstrapManager {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static Map<String, Bootstrapper> bootstrappers = new ConcurrentHashMap<String, Bootstrapper>();

    public static void register(String serviceName, ServiceInstaller installer) {
        if (installer == null) {
            throw new IllegalArgumentException("Installer cannot be null");
        }

        Bootstrapper bootstrapper = bootstrappers.get(serviceName);
        if (bootstrapper == null) {
            bootstrapper = new Bootstrapper(serviceName, installer);
            bootstrappers.put(serviceName, bootstrapper);
        } else {
            bootstrapper.setInstaller(installer);
        }
    }

    public static void bootstrap(ServiceScope scope) throws Exception {
        Bootstrapper bootstrapper = bootstrappers.get(scope.getServiceName());
        if (bootstrapper == null) {
            throw new IllegalArgumentException("Must register installer for service " + scope.getServiceName());
        }
        bootstrapper.bootstrap(scope.getDataVersion());
    }

    public static void reset(String serviceName) {
        bootstrappers.remove(serviceName);
    }

    public static class Bootstrapper {
        private boolean bootstrapped;
        private ServiceInstaller installer;
        private final String logPrefix;
        private final String serviceName;

        public Bootstrapper(String serviceName, ServiceInstaller installer) {
            this.installer = installer;
            this.logPrefix = String.format("[Service=%s] ", serviceName);
            this.serviceName = serviceName;
        }

        public void setInstaller(ServiceInstaller installer) {
            this.installer = installer;
        }

        public void bootstrap(int executableVersion) throws Exception {
            if (!bootstrapped) {
                synchronized (this) {
                    if (!bootstrapped) {
                        log.info("{}Running bootstrap", logPrefix);
                        install(executableVersion);
                        bootstrapped = true;
                    }
                }
            }
        }

        private void install(int executableVersion) throws Exception {
            Path serviceDirectoryPath = PathBuilder.buildServicePath(CamilleEnvironment.getPodId(), this.serviceName);
            InstallerAdaptor adaptor = new ServiceInstallerAdaptor(installer, serviceName);
            BootstrapUtil.install(adaptor, executableVersion, serviceDirectoryPath, true, logPrefix);
        }
    }
}