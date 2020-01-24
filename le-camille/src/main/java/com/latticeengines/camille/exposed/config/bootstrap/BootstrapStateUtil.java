package com.latticeengines.camille.exposed.config.bootstrap;

import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleTransaction;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public final class BootstrapStateUtil {

    protected BootstrapStateUtil() {
        throw new UnsupportedOperationException();
    }

    public static void initializeState(Path serviceDirectory, CamilleTransaction transaction, BootstrapState state) {
        Path path = serviceDirectory.append(PathConstants.BOOTSTRAP_STATE_FILE);
        transaction.create(path, DocumentUtils.toRawDocument(state), ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public static void initializeState(Path serviceDirectory, BootstrapState state) throws Exception {
        Path path = serviceDirectory.append(PathConstants.BOOTSTRAP_STATE_FILE);
        Camille camille = CamilleEnvironment.getCamille();
        try {
            camille.create(path, DocumentUtils.toRawDocument(state), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            throw new Exception(String.format("Unexpected failure attempting to initialize state at path %s", path,
                    state), e);
        }
    }

    public static BootstrapState getState(Path serviceDirectory) throws Exception {
        Path path = serviceDirectory.append(PathConstants.BOOTSTRAP_STATE_FILE);
        try {
            Camille camille = CamilleEnvironment.getCamille();
            Document raw = camille.get(path);
            return DocumentUtils.toTypesafeDocument(raw, BootstrapState.class);
        } catch (KeeperException.NoNodeException e) {
            return BootstrapState.createInitialState();
        } catch (Exception e) {
            throw new Exception(String.format("Unexpected failure attempting to retrieve bootstrap state at path %s",
                    path), e);
        }
    }

    public static BootstrapState getStateInCache(Path serviceDirectory, TreeCache cache) throws Exception {
        Path path = serviceDirectory.append(PathConstants.BOOTSTRAP_STATE_FILE);
        try {
            Camille camille = CamilleEnvironment.getCamille();
            Document raw = camille.getInCache(path, cache);
            return DocumentUtils.toTypesafeDocument(raw, BootstrapState.class);
        } catch (KeeperException.NoNodeException e) {
            return BootstrapState.createInitialState();
        } catch (Exception e) {
            throw new Exception(
                    String.format("Unexpected failure attempting to retrieve bootstrap state at path %s", path), e);
        }
    }

    public static void setState(Path serviceDirectory, BootstrapState state) throws Exception {
        Path path = serviceDirectory.append(PathConstants.BOOTSTRAP_STATE_FILE);
        Camille camille = CamilleEnvironment.getCamille();
        try {
            camille.upsert(path, DocumentUtils.toRawDocument(state), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            throw new Exception(String.format("Unexpected failure attempting to set state at path %s to state %s",
                    path, state), e);
        }
    }

    public static void setState(Path serviceDirectory, CamilleTransaction transaction, BootstrapState state) {
        Path path = serviceDirectory.append(PathConstants.BOOTSTRAP_STATE_FILE);
        transaction.set(path, DocumentUtils.toRawDocument(state));
    }
}
