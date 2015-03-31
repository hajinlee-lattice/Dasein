package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.CustomerSpaceServiceInstallerAdaptor;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.InstallerAdaptor;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceBootstrapManager {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static Map<String, Bootstrapper> bootstrappers = new ConcurrentHashMap<String, Bootstrapper>();

    public static void register(String serviceName, CustomerSpaceServiceInstaller installer,
            CustomerSpaceServiceUpgrader upgrader) {
        if (installer == null) {
            throw new IllegalArgumentException("Installer cannot be null");
        }
        if (upgrader == null) {
            throw new IllegalArgumentException("Upgrader cannot be null");
        }

        // Retrieve/Set the bootstrapper for the provided service
        Bootstrapper bootstrapper = bootstrappers.get(serviceName);
        if (bootstrapper == null) {
            bootstrapper = new Bootstrapper(serviceName, installer, upgrader);
            bootstrappers.put(serviceName, bootstrapper);
        } else {
            bootstrapper.setInstallerAndUpgrader(installer, upgrader);
        }
    }

    public static void bootstrap(CustomerSpaceServiceScope scope) throws Exception {
        Bootstrapper bootstrapper = bootstrappers.get(scope.getServiceName());
        if (bootstrapper == null) {
            throw new IllegalArgumentException("Must register an upgrader and an installer for service "
                    + scope.getServiceName());
        }
        bootstrapper.bootstrap(scope.getCustomerSpace(), scope.getDataVersion());
    }

    public static void reset(String serviceName, CustomerSpace space) {
        Bootstrapper bootstrapper = bootstrappers.get(serviceName);
        if (bootstrapper != null) {
            bootstrapper.reset(space);
        }
    }

    // XXX BootstrapState getBootstrapState(String serviceName, CustomerSpace
    // space)
    // XXX List<Pair<String,BootstrapState>> getBootstrapStates(CustomerSpace
    // space)

    public static class Bootstrapper {
        private final String serviceName;
        private CustomerSpaceServiceInstaller installer;
        private CustomerSpaceServiceUpgrader upgrader;
        private final ConcurrentMap<CustomerSpace, CustomerBootstrapper> customerBootstrappers = new ConcurrentHashMap<CustomerSpace, CustomerBootstrapper>();

        public Bootstrapper(String serviceName, CustomerSpaceServiceInstaller installer,
                CustomerSpaceServiceUpgrader upgrader) {
            this.serviceName = serviceName;
            this.installer = installer;
            this.upgrader = upgrader;
        }

        public void setInstallerAndUpgrader(CustomerSpaceServiceInstaller installer,
                CustomerSpaceServiceUpgrader upgrader) {
            this.installer = installer;
            this.upgrader = upgrader;
        }

        public void bootstrap(CustomerSpace space, int executableVersion) throws Exception {
            customerBootstrappers.putIfAbsent(space, new CustomerBootstrapper(space, serviceName, installer, upgrader));
            CustomerBootstrapper bootstrapper = customerBootstrappers.get(space);
            bootstrapper.bootstrap(executableVersion);
        }

        public synchronized void reset(CustomerSpace space) {
            customerBootstrappers.remove(space);
        }
    }

    /**
     * Bootstrapper for a specific customer space.
     */
    public static class CustomerBootstrapper {
        private final CustomerSpace space;
        private final String serviceName;
        private final CustomerSpaceServiceInstaller installer;
        private final CustomerSpaceServiceUpgrader upgrader;
        private final Path serviceDirectoryPath;
        private boolean bootstrapped;
        private final String logPrefix;

        public CustomerBootstrapper(CustomerSpace space, String serviceName, CustomerSpaceServiceInstaller installer,
                CustomerSpaceServiceUpgrader upgrader) {
            this.space = space;
            this.serviceName = serviceName;
            this.installer = installer;
            this.upgrader = upgrader;
            this.serviceDirectoryPath = getServiceDirectoryPath(space, serviceName);
            this.logPrefix = String.format("[Customer=%s, Service=%s] ", space, serviceName);
        }

        public void bootstrap(int executableVersion) throws Exception {
            if (!bootstrapped) {
                synchronized (this) {
                    if (!bootstrapped) {
                        log.info("{}Running bootstrap", logPrefix);
                        if (!SpaceLifecycleManager.exists(space.getContractId(), space.getTenantId(),
                                space.getSpaceId())) {
                            throw new RuntimeException("Customerspace " + space + " does not exist");
                        }
                        install(executableVersion);
                        upgrade(executableVersion);
                        bootstrapped = true;
                    }
                }
            }
        }

        private void install(int executableVersion) throws Exception {
            InstallerAdaptor adaptor = new CustomerSpaceServiceInstallerAdaptor(installer, space, serviceName);
            BootstrapUtil.install(adaptor, executableVersion, serviceDirectoryPath, false, logPrefix);
        }

        private void upgrade(int executableVersion) throws Exception {
            BootstrapUtil.upgrade(upgrader, executableVersion, serviceDirectoryPath, space, serviceName, logPrefix);
        }

        private static Path getServiceDirectoryPath(CustomerSpace space, String serviceName) {
            return PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), space.getContractId(),
                    space.getTenantId(), space.getSpaceId(), serviceName);
        }

    }
}
