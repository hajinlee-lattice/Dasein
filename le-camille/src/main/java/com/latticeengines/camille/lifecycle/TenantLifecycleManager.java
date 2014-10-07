package com.latticeengines.camille.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.camille.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class TenantLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(String contractId, String tenantId) throws Exception {
        create(contractId, tenantId, null);
    }

    public static void create(String contractId, String tenantId, String defaultSpaceId) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path tenantsPath = PathBuilder.buildTenantsPath(CamilleEnvironment.getPodId(), contractId);
            camille.create(tenantsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Tenants path @ {}", tenantsPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path tenantPath = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId);
        try {
            camille.create(tenantPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Tenant @ {}", tenantPath);

            if (defaultSpaceId == null) {
                defaultSpaceId = SpaceLifecycleManager.createDefault(contractId, tenantId);
            } else {
                SpaceLifecycleManager.create(contractId, tenantId, defaultSpaceId);
            }

            // create default space file
            Path defaultSpacePath = tenantPath.append(PathConstants.DEFAULT_SPACE_FILE);
            Document defaultSpaceDoc = new Document(defaultSpaceId);
            try {
                camille.create(defaultSpacePath, defaultSpaceDoc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
                log.debug("created .default-space @ {}", defaultSpacePath);
            } catch (KeeperException.NodeExistsException e) {
                log.debug(".default-space already existed @ {}, forcing update", defaultSpacePath);
                camille.set(defaultSpacePath, defaultSpaceDoc, true);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Tenant already existed @ {}, ignoring create", tenantPath);

            if (defaultSpaceId != null) {
                log.debug("updating default space Id to {}", defaultSpaceId);
                SpaceLifecycleManager.create(contractId, tenantId, defaultSpaceId);
                if (SpaceLifecycleManager.exists(contractId, tenantId, defaultSpaceId)) {
                    setDefaultSpaceId(contractId, tenantId, defaultSpaceId);
                } else {
                    log.debug("No Space Exists with Id={}, ignoring.", defaultSpaceId);
                }
            }
        }
    }

    public static void setDefaultSpaceId(String contractId, String tenantId, String defaultSpaceId) throws Exception {
        if (defaultSpaceId == null) {
            IllegalArgumentException e = new IllegalArgumentException("defaultSpaceId cannot be null");
            log.error(e.getMessage(), e);
            throw e;
        }
        if (SpaceLifecycleManager.exists(contractId, tenantId, defaultSpaceId)) {
            CamilleEnvironment.getCamille().set(
                    PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId).append(
                            PathConstants.DEFAULT_SPACE_FILE), new Document(defaultSpaceId));
        } else {
            RuntimeException e = new RuntimeException(String.format("No Space exists with spaceId=%s", defaultSpaceId));
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public static String getDefaultSpaceId(String contractId, String tenantId) throws Exception {
        return CamilleEnvironment
                .getCamille()
                .get(PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId).append(
                        PathConstants.DEFAULT_SPACE_FILE)).getData();
    }

    public static void delete(String contractId, String tenantId) throws Exception {
        Path tenantPath = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId);
        try {
            CamilleEnvironment.getCamille().delete(tenantPath);
            log.debug("deleted Tenant @ {}", tenantPath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Tenant Existed @ {}, ignoring delete", tenantPath);
        }
    }

    public static boolean exists(String contractId, String tenantId) throws Exception {
        return CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId));
    }

    /**
     * @return A list of tenantIds
     */
    public static List<String> getAll(String contractId) throws IllegalArgumentException, Exception {
        List<Pair<Document, Path>> childPairs = CamilleEnvironment.getCamille().getChildren(
                PathBuilder.buildTenantsPath(CamilleEnvironment.getPodId(), contractId));
        Collections.sort(childPairs, new Comparator<Pair<Document, Path>>() {
            @Override
            public int compare(Pair<Document, Path> o1, Pair<Document, Path> o2) {
                return o1.getRight().getSuffix().compareTo(o2.getRight().getSuffix());
            }
        });
        List<String> out = new ArrayList<String>(childPairs.size());
        for (Pair<Document, Path> childPair : childPairs) {
            out.add(childPair.getRight().getSuffix());
        }
        return out;
    }
}
