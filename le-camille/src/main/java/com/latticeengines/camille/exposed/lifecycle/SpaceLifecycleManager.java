package com.latticeengines.camille.exposed.lifecycle;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
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

        Document flags = DocumentUtils.toRawDocument(info.featureFlags);
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

    public static List<Pair<String, CustomerSpaceInfo>> getAll(String contractId, String tenantId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId);

        List<Pair<String, CustomerSpaceInfo>> toReturn = new ArrayList<Pair<String, CustomerSpaceInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<Pair<Document, Path>> childPairs = c.getChildren(PathBuilder.buildCustomerSpacesPath(
                CamilleEnvironment.getPodId(), contractId, tenantId));

        for (Pair<Document, Path> childPair : childPairs) {
            String spaceId = childPair.getRight().getSuffix();
            toReturn.add(new MutablePair<String, CustomerSpaceInfo>(spaceId, getInfo(contractId, tenantId, spaceId)));
        }

        return toReturn;
    }

    public static List<Pair<CustomerSpace, CustomerSpaceInfo>> getAll() throws Exception {
        List<Pair<CustomerSpace, CustomerSpaceInfo>> toReturn = new ArrayList<Pair<CustomerSpace, CustomerSpaceInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<Pair<Document, Path>> contracts = c.getChildren(PathBuilder.buildContractsPath(CamilleEnvironment
                .getPodId()));
        for (Pair<Document, Path> contract : contracts) {
            String contractId = contract.getRight().getSuffix();
            List<Pair<Document, Path>> tenants = c.getChildren(PathBuilder.buildTenantsPath(
                    CamilleEnvironment.getPodId(), contractId));
            for (Pair<Document, Path> tenant : tenants) {
                String tenantId = tenant.getRight().getSuffix();
                List<Pair<Document, Path>> spaces = c.getChildren(PathBuilder.buildCustomerSpacesPath(
                        CamilleEnvironment.getPodId(), contractId, tenantId));

                for (Pair<Document, Path> space : spaces) {
                    String spaceId = space.getRight().getSuffix();
                    toReturn.add(new MutablePair<CustomerSpace, CustomerSpaceInfo>(new CustomerSpace(contractId,
                            tenantId, spaceId), getInfo(contractId, tenantId, spaceId)));
                }
            }
        }

        return toReturn;
    }
}
