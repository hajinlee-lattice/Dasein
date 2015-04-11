package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleTransaction;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState.State;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;

public class BootstrapUtil {
    /**
     * 
     * @param force
     *            True to force the installation of new files to occur -
     *            deleting the old directory if it exists.
     * @param installer
     * @param executableVersion
     * @param serviceDirectoryPath
     * @param logPrefix
     * @throws Exception
     */
    public static void install(InstallerAdaptor installer, int executableVersion, Path serviceDirectoryPath,
            boolean force, String logPrefix) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();

        log.info("{}Running install of version {}", new Object[] { logPrefix, executableVersion });

        // If force is set, blow away the existing service directory if it
        // exists. It's not necessary to check the version file and only delete
        // it if the executableVersion differs - doing so would be an
        // unnecessary optimization.
        if (force) {
            if (camille.exists(serviceDirectoryPath)) {
                log.info("{}Service directory {} exists. Removing it before installing.", new Object[] { logPrefix,
                        serviceDirectoryPath });
                try {
                    camille.delete(serviceDirectoryPath);
                } catch (KeeperException.NoNodeException e) {
                    // pass
                }
            }
        }

        if (!camille.exists(serviceDirectoryPath)) {
            try {
                DocumentDirectory configurationDirectory = installer.install(executableVersion);
                if (configurationDirectory == null) {
                    throw new NullPointerException("Installer returned a null document directory");
                }

                CamilleTransaction transaction = new CamilleTransaction();

                // Create the parent Services directory
                camille.upsert(serviceDirectoryPath.parent(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

                // Create the service directory
                transaction.create(serviceDirectoryPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);

                // Initialize bootstrap state
                BootstrapStateUtil.initializeState(serviceDirectoryPath, transaction,
                        BootstrapState.constructOKState(executableVersion));

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
                BootstrapStateUtil.setState(
                        serviceDirectoryPath,
                        BootstrapState.constructErrorState(executableVersion, -1,
                                e.getMessage() + ": " + e.getStackTrace()));
                throw e;
            }
        }
    }

    private static void removeStateFile(DocumentDirectory directory) {
        Path stateFilePath = new Path(new String[]{PathConstants.BOOTSTRAP_STATE_FILE});
        if (directory.get(stateFilePath) != null) {
            directory.delete(stateFilePath);
        }
    }

    public static interface InstallerAdaptor {
        public DocumentDirectory install(int dataVersion);
    }

    public static class CustomerSpaceServiceInstallerAdaptor implements InstallerAdaptor {
        private final CustomerSpace space;
        private final String service;
        private final CustomerSpaceServiceInstaller installer;
        private final Map<String, String> properties;

        public CustomerSpaceServiceInstallerAdaptor(CustomerSpaceServiceInstaller installer, CustomerSpace space,
                String service, Map<String, String> properties) {
            this.space = space;
            this.service = service;
            this.installer = installer;
            this.properties = properties;
        }

        @Override
        public DocumentDirectory install(int dataVersion) {
            return installer.install(space, service, dataVersion, properties);
        }
    }

    public static class ServiceInstallerAdaptor implements InstallerAdaptor {
        private final String service;
        private final ServiceInstaller installer;
        private final Map<String, String> properties;

        public ServiceInstallerAdaptor(ServiceInstaller installer, String service, Map<String, String> properties) {
            this.service = service;
            this.installer = installer;
            this.properties = properties;
        }

        @Override
        public DocumentDirectory install(int dataVersion) {
            return installer.install(service, dataVersion, properties);
        }
    }

    public static void upgrade(CustomerSpaceServiceUpgrader upgrader, int executableVersion, Path serviceDirectoryPath,
            CustomerSpace space, String serviceName, String logPrefix) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        BootstrapState state = BootstrapStateUtil.getState(serviceDirectoryPath);

        try {
            if (state.installedVersion > executableVersion) {
                Exception vme = new VersionMismatchException(space, serviceName, state.installedVersion,
                        executableVersion);
                log.error("{}{}", logPrefix, vme.getMessage());
                throw vme;
            }

            if (state.installedVersion == executableVersion) {
                log.info("{}No need to upgrade - both executable and data are on version {}", logPrefix,
                        state.installedVersion);
                return;
            }

            if (state.installedVersion < executableVersion) {
                log.info("{}Running upgrade from version {} to version {}", new Object[] { logPrefix,
                        state.installedVersion, executableVersion });
                try {
                    DocumentDirectory source = camille.getDirectory(serviceDirectoryPath);

                    // Remove leading /etc/etc/Services/ServiceName/ from each
                    // path
                    source.makePathsLocal();

                    // Upgrade
                    DocumentDirectory upgraded = upgrader.upgrade(space, serviceName, state.installedVersion,
                            executableVersion, source, new HashMap<String, String>());

                    
                    // Perform a transaction
                    CamilleTransaction transaction = new CamilleTransaction();
                    
                    // - Delete all existing items in the hierarchy, leaf nodes
                    // -> root nodes
                    Path stateFilePath = new Path(new String[]{PathConstants.BOOTSTRAP_STATE_FILE});
                    Iterator<DocumentDirectory.Node> iter = source.leafFirstIterator();
                    while (iter.hasNext()) {
                        DocumentDirectory.Node node = iter.next();
                        if (!node.getPath().equals(stateFilePath)) {
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

                    // Update state
                    state.installedVersion = executableVersion;
                    state.desiredVersion = executableVersion;
                    state.state = State.OK;
                    state.errorMessage = null;
                    BootstrapStateUtil.setState(serviceDirectoryPath, transaction, state);

                    transaction.commit();
                } catch (KeeperException.BadVersionException e) {
                    log.warn("{}Another process already attempted to upgrade the configuration", logPrefix);

                    state = BootstrapStateUtil.getState(serviceDirectoryPath);
                    if (state.installedVersion != executableVersion) {
                        Exception vme = new VersionMismatchException(space, serviceName, state.installedVersion,
                                executableVersion);
                        log.error("{}{}", logPrefix, vme.getMessage());
                        throw vme;
                    }
                } catch (Exception e) {
                    log.error("{}Unexpected failure occurred attempting to upgrade configuration", logPrefix, e);
                    throw e;
                }
            }
        } catch (Exception e) {
            log.error("{}Unexpected failure occurred attempting to upgrade configuration", logPrefix, e);
            BootstrapStateUtil.setState(
                    serviceDirectoryPath,
                    BootstrapState.constructErrorState(executableVersion, state.installedVersion, e.getMessage() + ": "
                            + e.getStackTrace()));
            throw e;
        }
    }

    public static CustomerSpaceServiceInstaller sandbox(final CustomerSpaceServiceInstaller installer) {
        return new CustomerSpaceServiceInstaller() {

            @Override
            public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion,
                    Map<String, String> properties) {
                if (installer == null) {
                    return new DocumentDirectory();
                }

                DocumentDirectory toReturn = installer.install(space, serviceName, dataVersion, properties);
                if (toReturn == null) {
                    return new DocumentDirectory();
                }
                BootstrapUtil.removeStateFile(toReturn);
                return toReturn;
            }

            @Override
            public DocumentDirectory getDefaultConfiguration(String serviceName) {
                if (installer == null) {
                    return new DocumentDirectory();
                }
                DocumentDirectory toReturn = installer.getDefaultConfiguration(serviceName);
                BootstrapUtil.removeStateFile(toReturn);
                return toReturn;
            }
        };
    }

    public static CustomerSpaceServiceUpgrader sandbox(final CustomerSpaceServiceUpgrader upgrader) {
        return new CustomerSpaceServiceUpgrader() {

            @Override
            public DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion,
                    int targetVersion, DocumentDirectory source, Map<String, String> properties) {
                BootstrapUtil.removeStateFile(source);
                if (upgrader == null) {
                    return source;
                }

                DocumentDirectory toReturn = upgrader.upgrade(space, serviceName, sourceVersion, targetVersion, source, properties);
                if (toReturn == null) {
                    return new DocumentDirectory();
                }
                BootstrapUtil.removeStateFile(toReturn);
                return toReturn;
            }
        };
    }

    public static ServiceInstaller sandbox(final ServiceInstaller installer) {
        return new ServiceInstaller() {
            @Override
            public DocumentDirectory install(String serviceName, int dataVersion, Map<String, String> properties) {
                if (installer == null) {
                    return new DocumentDirectory();
                }
                DocumentDirectory toReturn = installer.install(serviceName, dataVersion, properties);
                if (toReturn == null) {
                    return new DocumentDirectory();
                }
                BootstrapUtil.removeStateFile(toReturn);
                return toReturn;
            }
        };
    }

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());
}
