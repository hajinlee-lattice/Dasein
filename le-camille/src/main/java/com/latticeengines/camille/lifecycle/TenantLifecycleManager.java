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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.camille.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class TenantLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final ObjectMapper mapper = new ObjectMapper();

    private static class DefaultSpace {
        private String spaceId = null;

        @SuppressWarnings("unused")
        public DefaultSpace() {
        }

        public DefaultSpace(String spaceId) {
            this.spaceId = spaceId;
        }

        public String getSpaceId() {
            return spaceId;
        }

        @SuppressWarnings("unused")
        public void setSpaceId(String spaceId) {
            this.spaceId = spaceId;
        }
    }

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
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Tenant already existed @ {}, ignoring create", tenantPath);
        }

        // create default space file
        Path defaultSpacePath = tenantPath.append(PathConstants.DEFAULT_SPACE_FILE);
        try {
            camille.create(defaultSpacePath, new Document(mapper.writeValueAsString(new DefaultSpace(defaultSpaceId))),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created .default-space @ {}", defaultSpacePath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug(".default-space already existed @ {}, ignoring create", defaultSpacePath);
        }
    }

    public static void setDefaultSpaceId(String contractId, String tenantId, String defaultSpaceId)
            throws JsonProcessingException, Exception {
        CamilleEnvironment.getCamille().set(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId).append(
                        PathConstants.DEFAULT_SPACE_FILE), new Document(mapper.writeValueAsString(new DefaultSpace(defaultSpaceId))));
    }

    public static String getDefaultSpaceId(String contractId, String tenantId) throws Exception {
        return mapper.readValue(
                CamilleEnvironment
                        .getCamille()
                        .get(PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId).append(
                                PathConstants.DEFAULT_SPACE_FILE)).getData(), DefaultSpace.class).getSpaceId();
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
