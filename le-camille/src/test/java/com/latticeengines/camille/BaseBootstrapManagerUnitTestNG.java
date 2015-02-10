package com.latticeengines.camille;

import java.util.concurrent.Semaphore;

import com.latticeengines.camille.exposed.config.bootstrap.Installer;
import com.latticeengines.camille.exposed.config.bootstrap.Upgrader;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public abstract class BaseBootstrapManagerUnitTestNG<T extends ConfigurationScope> {

    public void lock() throws Exception {
        // Force synchronous execution of all unit tests because of the nature
        // of how they deal with singleton state.
        semaphore.acquire();
    }

    public void unlock() throws Exception {
        semaphore.release();
    }

    public abstract T getTestScope();

    public static class Bootstrapper implements Installer, Upgrader {

        @Override
        public DocumentDirectory upgradeConfiguration(int sourceVersion, int targetVersion, DocumentDirectory source) {
            if (sourceVersion == 1 && targetVersion == 2) {
                return BaseBootstrapManagerUnitTestNG.getUpgradedConfiguration();
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public DocumentDirectory getInitialConfiguration(int dataVersion) {
            if (dataVersion == 1) {
                return BaseBootstrapManagerUnitTestNG.getInitialConfiguration();
            } else if (dataVersion == 2) {
                return getUpgradedConfiguration();
            } else {
                throw new IllegalStateException();
            }
        }
    }

    public static DocumentDirectory getInitialConfiguration() {
        final Integer version = 1;
        DocumentDirectory directory = new DocumentDirectory(new Path("/"));
        directory.add(new Path("/a"));
        directory.add(new Path("/a/b"), new Document(version.toString()));
        directory.add(new Path("/a/c"));
        directory.add(new Path("/a/c/d"), new Document(version.toString()));
        return directory;
    }

    public static DocumentDirectory getUpgradedConfiguration() {
        final Integer version = 2;
        DocumentDirectory directory = new DocumentDirectory(new Path("/"));
        directory.add(new Path("/a"));
        directory.add(new Path("/a/b"), new Document(version.toString()));
        directory.add(new Path("/a/d"), new Document(version.toString()));
        directory.add(new Path("/a/e"), new Document(version.toString()));
        return directory;
    }

    protected boolean configurationEquals(DocumentDirectory configurationFromZK, DocumentDirectory sourceConfiguration) {
        // TODO Eventually will not be necessary once ConfigurationControllers
        // omit hidden files
        configurationFromZK.delete(new Path("/").append(PathConstants.SERVICE_DATA_VERSION_FILE));
        return configurationFromZK.equals(sourceConfiguration);
    }

    private final static Semaphore semaphore = new Semaphore(1);
}
