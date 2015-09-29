package com.latticeengines.camille.exposed.lifecycle;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class TenantLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(String contractId, String tenantId, TenantInfo tenantInfo, String defaultSpaceId,
            CustomerSpaceInfo defaultSpaceInfo) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId, defaultSpaceId);

        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path tenantsPath = PathBuilder.buildTenantsPath(CamilleEnvironment.getPodId(), contractId);
            camille.create(tenantsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Tenants path @ {}", tenantsPath);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }

        Path tenantPath = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId);
        try {
            camille.create(tenantPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Tenant @ {}", tenantPath);

            SpaceLifecycleManager.create(contractId, tenantId, defaultSpaceId, defaultSpaceInfo);

            // create default space file
            Path defaultSpacePath = tenantPath.append(PathConstants.DEFAULT_SPACE_FILE);
            Document defaultSpaceDoc = new Document(defaultSpaceId);
            try {
                camille.create(defaultSpacePath, defaultSpaceDoc, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
                log.debug("created .default-space @ {}", defaultSpacePath);
            } catch (KeeperException.NodeExistsException e) {
                log.debug(".default-space already existed @ {}, forcing update", defaultSpacePath);
                camille.set(defaultSpacePath, defaultSpaceDoc, true);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Tenant already existed @ {}, ignoring create", tenantPath);

            if (defaultSpaceId != null) {
                log.debug("updating default space Id to {}", defaultSpaceId);
                SpaceLifecycleManager.create(contractId, tenantId, defaultSpaceId, defaultSpaceInfo);
                setDefaultSpaceId(contractId, tenantId, defaultSpaceId);
            }
        }

        Document properties = DocumentUtils.toRawDocument(tenantInfo.properties);
        Path propertiesPath = tenantPath.append(PathConstants.PROPERTIES_FILE);
        camille.upsert(propertiesPath, properties, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        log.debug("created properties @ {}", propertiesPath);

    }

    public static void setDefaultSpaceId(String contractId, String tenantId, String defaultSpaceId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId, defaultSpaceId);

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
        LifecycleUtils.validateIds(contractId, tenantId);

        return CamilleEnvironment
                .getCamille()
                .get(PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId).append(
                        PathConstants.DEFAULT_SPACE_FILE)).getData();
    }

    public static void delete(String contractId, String tenantId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId);

        Path tenantPath = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId);
        try {
            CamilleEnvironment.getCamille().delete(tenantPath);
            log.debug("deleted Tenant @ {}", tenantPath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Tenant Existed @ {}, ignoring delete", tenantPath);
        }
    }

    public static boolean exists(String contractId, String tenantId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId);

        return CamilleEnvironment.getCamille().exists(
                PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId));
    }

    public static List<AbstractMap.SimpleEntry<String, TenantInfo>> getAll(String contractId) throws Exception {
        LifecycleUtils.validateIds(contractId);
        List<AbstractMap.SimpleEntry<String, TenantInfo>> toReturn = new ArrayList<AbstractMap.SimpleEntry<String, TenantInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<AbstractMap.SimpleEntry<Document, Path>> childPairs = c.getChildren(PathBuilder.buildTenantsPath(
                CamilleEnvironment.getPodId(), contractId));

        for (Map.Entry<Document, Path> childPair : childPairs) {
            TenantProperties properties = null;
            try {
                Document tenantPropertiesDocument = c.get(childPair.getValue().append(PathConstants.PROPERTIES_FILE));
                properties = DocumentUtils.toTypesafeDocument(tenantPropertiesDocument, TenantProperties.class);
                if (properties != null) {
                    TenantInfo tenantInfo = new TenantInfo(properties);
                    toReturn.add(new AbstractMap.SimpleEntry<>(childPair.getValue().getSuffix(), tenantInfo));
                }
            } catch (Exception ex) {
                log.warn("Failed to retrieve the properties.json at path="
                        + (childPair.getValue() != null ? childPair.getValue().toString() : ""), ex);
            }
        }

        return toReturn;
    }

    public static TenantInfo getInfo(String contractId, String tenantId) throws Exception {
        LifecycleUtils.validateIds(contractId);
        LifecycleUtils.validateIds(tenantId);
        Camille c = CamilleEnvironment.getCamille();

        Path tenantPath = PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), contractId, tenantId);
        Document tenantPropertiesDocument = c.get(tenantPath.append(PathConstants.PROPERTIES_FILE));
        TenantProperties properties = DocumentUtils
                .toTypesafeDocument(tenantPropertiesDocument, TenantProperties.class);

        return new TenantInfo(properties);
    }
}
