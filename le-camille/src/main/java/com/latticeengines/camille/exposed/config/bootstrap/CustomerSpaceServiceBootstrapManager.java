package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleTransaction;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceBootstrapManager {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static Map<String, Bootstrapper> bootstrappers = new HashMap<String, Bootstrapper>();

    public static synchronized void register(String serviceName, Installer installer, Upgrader upgrader) {
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

    public static synchronized void reset(String serviceName, CustomerSpace space) {
        Bootstrapper bootstrapper = bootstrappers.get(serviceName);
        if (bootstrapper != null) {
            bootstrapper.reset(space);
        }
    }

    public static class Bootstrapper {
        private final String serviceName;
        private Installer installer;
        private Upgrader upgrader;
        private final ConcurrentMap<CustomerSpace, CustomerBootstrapper> customerBootstrappers = new ConcurrentHashMap<CustomerSpace, CustomerBootstrapper>();

        public Bootstrapper(String serviceName, Installer installer, Upgrader upgrader) {
            this.serviceName = serviceName;
            this.installer = installer;
            this.upgrader = upgrader;
        }

        public void setInstallerAndUpgrader(Installer installer, Upgrader upgrader) {
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
        private final Installer installer;
        private final Upgrader upgrader;
        private final Path serviceDirectoryPath;
        private final Path dataVersionFilePath;
        private boolean bootstrapped;
        private final Camille camille;
        private final String logPrefix;

        public CustomerBootstrapper(CustomerSpace space, String serviceName, Installer installer, Upgrader upgrader) {
            this.space = space;
            this.serviceName = serviceName;
            this.installer = installer;
            this.upgrader = upgrader;
            this.camille = CamilleEnvironment.getCamille();
            this.serviceDirectoryPath = getServiceDirectoryPath(space, serviceName);
            this.dataVersionFilePath = getVersionFilePath(space, serviceName);
            this.logPrefix = String.format("[Customer=%s, Service=%s] ", space, serviceName);
        }

        public void bootstrap(int executableVersion) throws Exception {
            if (!bootstrapped) {
                synchronized (this) {
                    if (!bootstrapped) {
                        log.info("{}Running bootstrap", logPrefix);
                        if (!SpaceLifecycleManager.exists(space.getContractId(), space.getTenantId(), space.getSpaceId())) {
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
            if (!camille.exists(serviceDirectoryPath)) {
                try {
                    log.info("{}Service directory {} does not exist. Running initial install of version {}",
                            new Object[] { logPrefix, serviceDirectoryPath, executableVersion });

                    DocumentDirectory configurationDirectory = installer.getInitialConfiguration(executableVersion);
                    if (configurationDirectory == null) {
                        throw new NullPointerException("Installer returned a null document directory");
                    }

                    CamilleTransaction transaction = new CamilleTransaction();

                    // Create the service directory
                    transaction.create(serviceDirectoryPath, new Document(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

                    // Create the version file
                    String version = new Integer(executableVersion).toString();
                    transaction.create(dataVersionFilePath, new Document(version), ZooDefs.Ids.OPEN_ACL_UNSAFE);

                    // Create everything under it
                    Iterator<DocumentDirectory.Node> iter = configurationDirectory.breadthFirstIterator();
                    while (iter.hasNext()) {
                        DocumentDirectory.Node node = iter.next();
                        transaction.create(node.getPath().prefix(serviceDirectoryPath), node.getDocument(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE);
                    }

                    transaction.commit();

                } catch (KeeperException.NodeExistsException e) {
                    log.warn("{}Another process already installed the initial configuration", logPrefix, e);
                } catch (Exception e) {
                    log.error("{}Unexpected failure occurred attempting to install initial configuration", logPrefix, e);
                    throw e;
                }
            }
        }

        private void upgrade(int executableVersion) throws Exception {
            Document versionDocument = camille.get(dataVersionFilePath);
            int dataVersion = Integer.parseInt(versionDocument.getData());

            if (dataVersion > executableVersion) {
                Exception vme = new VersionMismatchException(space, serviceName, dataVersion, executableVersion);
                log.error("{}{}", logPrefix, vme.getMessage());
                throw vme;
            }

            if (dataVersion == executableVersion) {
                log.info("{}No need to upgrade - both executable and data are on version {}", logPrefix, dataVersion);
                return;
            }

            if (dataVersion < executableVersion) {
                log.info("{}Running upgrade from version {} to version {}", new Object[] { logPrefix, dataVersion,
                        executableVersion });
                try {
                    Camille camille = CamilleEnvironment.getCamille();
                    DocumentDirectory source = camille.getDirectory(serviceDirectoryPath);

                    // Remove leading /etc/etc/Services/ServiceName/ from each
                    // path
                    source.makePathsLocal();
                    // TODO filter out invisible documents

                    // Upgrade
                    DocumentDirectory upgraded = upgrader.upgradeConfiguration(dataVersion, executableVersion, source);
                    if (upgraded == null) {
                        throw new NullPointerException("Upgrader returned a null document directory");
                    }

                    // Perform a transaction
                    CamilleTransaction transaction = new CamilleTransaction();

                    // - Delete all existing items in the hierarchy, leaf nodes
                    // -> root nodes
                    Iterator<DocumentDirectory.Node> iter = source.leafFirstIterator();
                    while (iter.hasNext()) {
                        DocumentDirectory.Node node = iter.next();
                        if (!node.getPath().equals(dataVersionFilePath.local(serviceDirectoryPath))) {
                            transaction.delete(node.getPath().prefix(serviceDirectoryPath));
                        }
                    }

                    // - Create the new items in the hierarchy, root nodes ->
                    // leaf nodes
                    iter = upgraded.breadthFirstIterator();
                    while (iter.hasNext()) {
                        DocumentDirectory.Node node = iter.next();
                        transaction.create(node.getPath().prefix(serviceDirectoryPath), node.getDocument(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE);
                    }

                    // Write the .version file. This will fail if the version
                    // document has already been
                    // written to by another thread
                    versionDocument.setData(new Integer(executableVersion).toString());
                    transaction.set(dataVersionFilePath, versionDocument);

                    transaction.commit();
                } catch (KeeperException.BadVersionException e) {
                    log.warn("{}Another process already attempted to upgrade the configuration", logPrefix);

                    versionDocument = camille.get(dataVersionFilePath);
                    dataVersion = Integer.parseInt(versionDocument.getData());

                    if (dataVersion != executableVersion) {
                        Exception vme = new VersionMismatchException(space, serviceName, dataVersion, executableVersion);
                        log.error("{}{}", logPrefix, vme.getMessage());
                        throw vme;
                    }
                } catch (Exception e) {
                    log.error("{}Unexpected failure occurred attempting to upgrade configuration", logPrefix, e);
                    throw e;
                }
            }
        }

        private static Path getServiceDirectoryPath(CustomerSpace space, String serviceName) {
            return PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), space.getContractId(),
                    space.getTenantId(), space.getSpaceId(), serviceName);
        }

        private static Path getVersionFilePath(CustomerSpace space, String serviceName) {
            return getServiceDirectoryPath(space, serviceName).append(PathConstants.SERVICE_DATA_VERSION_FILE);
        }
    }
}
