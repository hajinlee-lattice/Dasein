package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.Iterator;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleTransaction;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

public class BootstrapUtil {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

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
    public static void install(Installer installer, int executableVersion, Path serviceDirectoryPath, boolean force,
            String logPrefix) throws Exception {
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
                Path dataVersionFilePath = serviceDirectoryPath.append(PathConstants.SERVICE_DATA_VERSION_FILE);

                DocumentDirectory configurationDirectory = installer.getInitialConfiguration(executableVersion);
                if (configurationDirectory == null) {
                    throw new NullPointerException("Installer returned a null document directory");
                }

                CamilleTransaction transaction = new CamilleTransaction();

                // Create the parent Services directory
                camille.upsert(serviceDirectoryPath.parent(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

                // Create the service directory
                transaction.create(serviceDirectoryPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);

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

    public static void upgrade(Upgrader upgrader, int executableVersion, Path serviceDirectoryPath,
            CustomerSpace space, String serviceName, String logPrefix) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path dataVersionFilePath = serviceDirectoryPath.append(PathConstants.SERVICE_DATA_VERSION_FILE);

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

}
