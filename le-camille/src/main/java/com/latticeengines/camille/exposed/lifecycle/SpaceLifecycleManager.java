package com.latticeengines.camille.exposed.lifecycle;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class SpaceLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @SuppressWarnings("deprecation")
    public static void create(String contractId, String tenantId, String spaceId, CustomerSpaceInfo info)
            throws Exception {

        LifecycleUtils.validateIds(contractId, tenantId, spaceId);

        Camille camille = CamilleEnvironment.getCamille();

        if (info == null || info.properties == null || info.featureFlags == null) {
            throw new NullPointerException("properties and feature flags cannot be null");
        }

        try {
            Path spacesPath = PathBuilder.buildCustomerSpacesPath(CamilleEnvironment.getPodId(), contractId, tenantId);
            camille.create(spacesPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Spaces path @ {}", spacesPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path spacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                spaceId);
        try {
            camille.create(spacePath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Space @ {}", spacePath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Space already existed @ {}, ignoring create", spacePath);
        }

        Document properties = DocumentUtils.toRawDocument(info.properties);
        Path propertiesPath = spacePath.append(PathConstants.PROPERTIES_FILE);
        camille.upsert(propertiesPath, properties, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        log.debug("created properties @ {}", propertiesPath);

        Document flags = new Document(info.featureFlags);
        Path flagsPath = spacePath.append(PathConstants.FEATURE_FLAGS_FILE);
        camille.upsert(flagsPath, flags, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        log.debug("created feature flags @ {}", flagsPath);
    }

    public static void delete(String contractId, String tenantId, String spaceId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId, spaceId);

        Path spacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                spaceId);
        try {
            CamilleEnvironment.getCamille().delete(spacePath);
            log.debug("deleted Space @ {}", spacePath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Space Existed @ {}, ignoring delete", spacePath);
        }
    }

    public static boolean exists(String contractId, String tenantId, String spaceId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId, spaceId);

        return CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId));
    }

    @SuppressWarnings("deprecation")
    public static CustomerSpaceInfo getInfo(String contractId, String tenantId, String spaceId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId, spaceId);
        Camille c = CamilleEnvironment.getCamille();

        Path spacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                spaceId);
        Document spacePropertiesDocument = c.get(spacePath.append(PathConstants.PROPERTIES_FILE));
        CustomerSpaceProperties properties = DocumentUtils.toTypesafeDocument(spacePropertiesDocument,
                CustomerSpaceProperties.class);

        Document spaceFlagsDocument = c.get(spacePath.append(PathConstants.FEATURE_FLAGS_FILE));
        String flags = spaceFlagsDocument.getData();

        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, flags);
        return spaceInfo;
    }

    public static List<AbstractMap.SimpleEntry<String, CustomerSpaceInfo>> getAll(String contractId, String tenantId)
            throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId);

        List<AbstractMap.SimpleEntry<String, CustomerSpaceInfo>> toReturn = new ArrayList<AbstractMap.SimpleEntry<String, CustomerSpaceInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<AbstractMap.SimpleEntry<Document, Path>> childPairs = c.getChildren(PathBuilder.buildCustomerSpacesPath(
                CamilleEnvironment.getPodId(), contractId, tenantId));

        for (AbstractMap.SimpleEntry<Document, Path> childPair : childPairs) {
            String spaceId = "";
            try {
                spaceId = childPair.getValue().getSuffix();
                toReturn.add(new AbstractMap.SimpleEntry<String, CustomerSpaceInfo>(spaceId, getInfo(contractId,
                        tenantId, spaceId)));
            } catch (Exception ex) {
                log.warn("Can not add spaceId=" + spaceId, ex);
            }
        }

        return toReturn;
    }

    public static List<AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>> getAll() throws Exception {
        List<AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>> toReturn = new ArrayList<AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<AbstractMap.SimpleEntry<Document, Path>> contracts = c.getChildren(PathBuilder
                .buildContractsPath(CamilleEnvironment.getPodId()));
        for (AbstractMap.SimpleEntry<Document, Path> contract : contracts) {
            String contractId = contract.getValue().getSuffix();
            List<AbstractMap.SimpleEntry<Document, Path>> tenants = c.getChildren(PathBuilder.buildTenantsPath(
                    CamilleEnvironment.getPodId(), contractId));
            for (AbstractMap.SimpleEntry<Document, Path> tenant : tenants) {
                String tenantId = tenant.getValue().getSuffix();
                List<AbstractMap.SimpleEntry<Document, Path>> spaces = c.getChildren(PathBuilder
                        .buildCustomerSpacesPath(CamilleEnvironment.getPodId(), contractId, tenantId));

                for (AbstractMap.SimpleEntry<Document, Path> space : spaces) {
                    String spaceId = "";
                    try {
                        spaceId = space.getValue().getSuffix();
                        toReturn.add(new AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>(new CustomerSpace(
                                contractId, tenantId, spaceId), getInfo(contractId, tenantId, spaceId)));
                    } catch (Exception ex) {
                        log.warn("Can not add spaceId=" + spaceId, ex);
                    }
                }
            }
        }

        return toReturn;
    }
}
