package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.latticeengines.domain.exposed.camille.bootstrap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.CustomerSpaceServiceInstallerAdaptor;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.InstallerAdaptor;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.UpgraderAdaptor;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.DestroyerAdaptor;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceBootstrapManager {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static Map<String, Bootstrapper> bootstrappers = new ConcurrentHashMap<String, Bootstrapper>();

    public static void register(String serviceName, ServiceProperties properties,
            CustomerSpaceServiceInstaller installer, CustomerSpaceServiceUpgrader upgrader,
            CustomerSpaceServiceDestroyer destroyer) {

        // Retrieve/Set the bootstrapper for the provided service
        Bootstrapper bootstrapper = bootstrappers.get(serviceName);
        if (bootstrapper == null) {
            bootstrapper = new Bootstrapper(serviceName, properties, installer, upgrader, destroyer);
            bootstrappers.put(serviceName, bootstrapper);
        } else {
            bootstrapper.set(properties, installer, upgrader, destroyer);
        }
    }

    public static void bootstrap(CustomerSpaceServiceScope scope) throws Exception {
        Bootstrapper bootstrapper = bootstrappers.get(scope.getServiceName());
        if (bootstrapper == null) {
            throw new IllegalArgumentException("Must register an upgrader and an installer for service "
                    + scope.getServiceName());
        }
        bootstrapper.bootstrap(scope.getCustomerSpace(), scope.getProperties());
    }

    public static void resetAll() {
        bootstrappers.clear();
    }

    public static void reset(String serviceName, CustomerSpace space) {
        Bootstrapper bootstrapper = bootstrappers.get(serviceName);
        if (bootstrapper != null) {
            bootstrapper.reset(space);
        }
    }

    public static BootstrapState getBootstrapState(String serviceName, CustomerSpace space) throws Exception {
        Path serviceDirectoryPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), space,
                serviceName);
        try {
            return BootstrapStateUtil.getState(serviceDirectoryPath);
        } catch (Exception e) {
            throw new Exception(String.format(
                    "Error encountered retrieving bootstrap state for space %s and service %s", space, serviceName), e);
        }

    }

    public static List<AbstractMap.SimpleEntry<String, BootstrapState>> getBootstrapStates(CustomerSpace space)
            throws Exception {
        Path servicesDirectoryPath = PathBuilder.buildCustomerSpaceServicesPath(CamilleEnvironment.getPodId(), space);
        Camille camille = CamilleEnvironment.getCamille();

        List<AbstractMap.SimpleEntry<String, BootstrapState>> toReturn = new ArrayList<AbstractMap.SimpleEntry<String, BootstrapState>>();
        try {
            List<AbstractMap.SimpleEntry<Document, Path>> children = camille.getChildren(servicesDirectoryPath);
            for (AbstractMap.SimpleEntry<Document, Path> child : children) {
                String serviceName = child.getValue().getSuffix();
                BootstrapState state = getBootstrapState(serviceName, space);
                toReturn.add(new AbstractMap.SimpleEntry<String, BootstrapState>(serviceName, state));
            }
        } catch (Exception e) {
            throw new Exception(String.format("Error encountered retrieving bootstrap states for space %s", space), e);
        }

        return toReturn;
    }

    public static class Bootstrapper {
        private final String serviceName;
        private CustomerSpaceServiceInstaller installer;
        private CustomerSpaceServiceUpgrader upgrader;
        private CustomerSpaceServiceDestroyer destroyer;
        private ServiceProperties properties;
        private final ConcurrentMap<CustomerSpace, CustomerBootstrapper> customerBootstrappers = new ConcurrentHashMap<CustomerSpace, CustomerBootstrapper>();

        public Bootstrapper(String serviceName, ServiceProperties properties, CustomerSpaceServiceInstaller installer,
                CustomerSpaceServiceUpgrader upgrader, CustomerSpaceServiceDestroyer destroyer) {
            this.serviceName = serviceName;
            this.properties = properties;
            this.installer = installer;
            this.upgrader = upgrader;
            this.destroyer = destroyer;
        }

        public void set(ServiceProperties properties, CustomerSpaceServiceInstaller installer,
                CustomerSpaceServiceUpgrader upgrader, CustomerSpaceServiceDestroyer destroyer) {
            this.properties = properties;
            this.installer = installer;
            this.upgrader = upgrader;
            this.destroyer = destroyer;
        }

        public void bootstrap(CustomerSpace space, Map<String, String> bootstrapProperties) throws Exception {
            customerBootstrappers.put(space, new CustomerBootstrapper(space, serviceName, installer,
                    upgrader, destroyer, bootstrapProperties));
            CustomerBootstrapper bootstrapper = customerBootstrappers.get(space);
            bootstrapper.bootstrap(this.properties.dataVersion);
        }

        public synchronized void reset(CustomerSpace space) {
            customerBootstrappers.remove(space);
        }

        public CustomerSpaceServiceInstaller getInstaller() {
            return installer;
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
        private final CustomerSpaceServiceDestroyer destroyer;
        private final Path serviceDirectoryPath;
        private final String logPrefix;
        private final Map<String, String> bootstrapProperties;

        public CustomerBootstrapper(CustomerSpace space, String serviceName, CustomerSpaceServiceInstaller installer,
                CustomerSpaceServiceUpgrader upgrader, CustomerSpaceServiceDestroyer destroyer,
                Map<String, String> bootstrapProperties) {
            this.space = space;
            this.serviceName = serviceName;
            this.installer = installer;
            this.upgrader = upgrader;
            this.destroyer = destroyer;
            this.serviceDirectoryPath = getServiceDirectoryPath(space, serviceName);
            this.logPrefix = String.format("[Customer=%s, Service=%s] ", space, serviceName);
            this.bootstrapProperties = bootstrapProperties;
        }

        public void bootstrap(int executableVersion) throws Exception {
            log.info("{}On entry to bootstrap", logPrefix);
            synchronized (this) {
                log.info("{}Running bootstrap", logPrefix);
                if(bootstrapProperties.containsKey(BootstrapPropertyConstant.BOOTSTRAP_COMMAND)) {
                    if (bootstrapProperties.get(BootstrapPropertyConstant.BOOTSTRAP_COMMAND).equals
                            (BootstrapPropertyConstant.BOOTSTRAP_UNINSTALL)) {
                        destroy(executableVersion);
                    }
                } else {
                    if (!SpaceLifecycleManager.exists(space.getContractId(), space.getTenantId(),
                            space.getSpaceId())) {
                        throw new RuntimeException("Customerspace " + space + " does not exist");
                    }
                    install(executableVersion);
                    upgrade(executableVersion);
                }
            }
        }

        private void install(int executableVersion) throws Exception {
            InstallerAdaptor adaptor = new CustomerSpaceServiceInstallerAdaptor(installer, space, serviceName,
                    bootstrapProperties);
            BootstrapUtil.install(adaptor, executableVersion, serviceDirectoryPath, logPrefix);
        }

        private void upgrade(int executableVersion) throws Exception {
            UpgraderAdaptor adaptor = new UpgraderAdaptor(upgrader, space, serviceName, bootstrapProperties);
            BootstrapUtil.upgrade(adaptor, executableVersion, serviceDirectoryPath, space, serviceName, logPrefix);
        }

        private void destroy(int executableVersion) throws Exception {
            DestroyerAdaptor adaptor = new DestroyerAdaptor(destroyer, space, serviceName);
            BootstrapUtil.destroy(adaptor, executableVersion, serviceDirectoryPath, space, serviceName, logPrefix);
        }

        private static Path getServiceDirectoryPath(CustomerSpace space, String serviceName) {
            return PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), space.getContractId(),
                    space.getTenantId(), space.getSpaceId(), serviceName);
        }

    }
}
