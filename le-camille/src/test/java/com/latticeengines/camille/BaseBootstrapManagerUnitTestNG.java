package com.latticeengines.camille;

import java.util.Map;
import java.util.concurrent.Semaphore;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState.State;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public abstract class BaseBootstrapManagerUnitTestNG<T extends ConfigurationScope> {
    static final int INITIAL_VERSION = 1;
    static final int UPGRADED_VERSION = 2;

    private static final String INITIAL_VERSION_STRING = "1.0";
    private static final String UPGRADED_VERSION_STRING = "1.1";

    static final ServiceProperties INITIAL_VERSION_PROPERTIES = new ServiceProperties(INITIAL_VERSION,
            INITIAL_VERSION_STRING);
    static final ServiceProperties UPGRADED_VERSION_PROPERTIES = new ServiceProperties(UPGRADED_VERSION,
            UPGRADED_VERSION_STRING);

    private static final Semaphore semaphore = new Semaphore(1);

    public void lock() throws Exception {
        // Force synchronous execution of all unit tests because of the nature
        // of how they deal with singleton state.
        semaphore.acquire();
    }

    public void unlock() {
        semaphore.release();
    }

    public abstract T getTestScope();

    public abstract BootstrapState getState() throws Exception;

    public static class Bootstrapper implements CustomerSpaceServiceInstaller, CustomerSpaceServiceUpgrader,
            CustomerSpaceServiceDestroyer, ServiceInstaller {

        @Override
        public DocumentDirectory upgrade(CustomerSpace space, String service, int sourceVersion, int targetVersion,
                DocumentDirectory source, Map<String, String> properties) {
            if (sourceVersion == INITIAL_VERSION && targetVersion == UPGRADED_VERSION) {
                return BaseBootstrapManagerUnitTestNG.getUpgradedConfiguration();
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public DocumentDirectory install(CustomerSpace space, String service, int dataVersion,
                Map<String, String> properties) {
            if (dataVersion == INITIAL_VERSION) {
                return BaseBootstrapManagerUnitTestNG.getInitialConfiguration();
            } else if (dataVersion == UPGRADED_VERSION) {
                return getUpgradedConfiguration();
            } else {
                throw new IllegalStateException();
            }
        }

        public DocumentDirectory install(String service, int dataVersion, Map<String, String> properties) {
            if (dataVersion == INITIAL_VERSION) {
                return BaseBootstrapManagerUnitTestNG.getInitialConfiguration();
            } else if (dataVersion == UPGRADED_VERSION) {
                return getUpgradedConfiguration();
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public boolean destroy(CustomerSpace space, String serviceName) {
            return false;
        }
    }

    public static class EvilBootstrapper implements CustomerSpaceServiceInstaller, CustomerSpaceServiceUpgrader,
            CustomerSpaceServiceDestroyer, ServiceInstaller {
        @Override
        public DocumentDirectory install(String serviceName, int dataVersion, Map<String, String> properties) {
            throw new RuntimeException("Death!");
        }

        @Override
        public DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion, int targetVersion,
                DocumentDirectory source, Map<String, String> properties) {
            throw new RuntimeException("Famine!");
        }

        @Override
        public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion,
                Map<String, String> properties) {
            throw new RuntimeException("VisiDB!");
        }

        @Override
        public boolean destroy(CustomerSpace space, String serviceName) {
            throw new RuntimeException("Destroyer!");
        }
    }

    private static DocumentDirectory getInitialConfiguration() {
        DocumentDirectory directory = new DocumentDirectory(new Path("/"));
        directory.add(new Path("/a"));
        directory.add(new Path("/a/b"), new Document(Integer.toString(INITIAL_VERSION)));
        directory.add(new Path("/a/c"));
        directory.add(new Path("/a/c/d"), new Document(Integer.toString(INITIAL_VERSION)));
        return directory;
    }

    private static DocumentDirectory getUpgradedConfiguration() {
        DocumentDirectory directory = new DocumentDirectory(new Path("/"));
        directory.add(new Path("/a"));
        directory.add(new Path("/a/b"), new Document(Integer.toString(UPGRADED_VERSION)));
        directory.add(new Path("/a/d"), new Document(Integer.toString(UPGRADED_VERSION)));
        directory.add(new Path("/a/e"), new Document(Integer.toString(UPGRADED_VERSION)));
        return directory;
    }

    private static DocumentDirectory getConfiguration(int version) {
        switch (version) {
        case INITIAL_VERSION:
            return getInitialConfiguration();
        case UPGRADED_VERSION:
            return getUpgradedConfiguration();
        default:
            throw new RuntimeException(String.format("Invalid version specified to getConfiguration: %d", version));
        }
    }

    boolean serviceIsInState(State state, int desiredAndInstalledVersion) throws Exception {
        return serviceIsInState(state, desiredAndInstalledVersion, desiredAndInstalledVersion);
    }

    boolean serviceIsInState(State state, int desiredVersion, int installedVersion) throws Exception {
        T scope = getTestScope();
        // Retrieve state
        BootstrapState retrieved = getState();

        boolean equivalentState = retrieved.state == state && retrieved.installedVersion == installedVersion
                && retrieved.desiredVersion == desiredVersion;

        // If in error or initial state, just compare states
        if (state == State.ERROR || state == State.INITIAL) {
            return equivalentState;
        }

        // Otherwise compare both state and the configuration files
        ConfigurationController<T> controller = ConfigurationController.construct(scope);
        DocumentDirectory configuration = controller.getDirectory(new Path("/"));

        // TODO Eventually will not be necessary once ConfigurationControllers
        // omit hidden files
        configuration.delete(new Path("/").append(PathConstants.BOOTSTRAP_STATE_FILE));
        configuration.delete(new Path("/").append(PathConstants.BOOTSTRAP_LOCK));

        DocumentDirectory sourceConfiguration = getConfiguration(retrieved.installedVersion);
        return equivalentState && configuration.equals(sourceConfiguration);
    }
}
