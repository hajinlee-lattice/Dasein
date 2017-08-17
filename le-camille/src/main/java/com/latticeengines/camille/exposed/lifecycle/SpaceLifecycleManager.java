package com.latticeengines.camille.exposed.lifecycle;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.translators.PathTranslator;
import com.latticeengines.camille.exposed.translators.PathTranslatorFactory;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.scopes.PodDivisionScope;

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

    public static CustomerSpaceInfo getInfo(String contractId, String tenantId, String spaceId) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId, spaceId);
        Camille c = CamilleEnvironment.getCamille();

        Path spacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                spaceId);
        Document spacePropertiesDocument = c.get(spacePath.append(PathConstants.PROPERTIES_FILE));
        CustomerSpaceProperties properties = DocumentUtils.toTypesafeDocument(spacePropertiesDocument,
                CustomerSpaceProperties.class);

        Document spaceFlagsDocument = c.get(spacePath.append(PathConstants.FEATURE_FLAGS_FILE));
        Document featureFlagDefinitionDocument = c.get(buildFeatureFlagDefinitionPath());
        Path productsPath = PathBuilder
                .buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)
                .append(new Path("/" + PathConstants.SPACECONFIGURATION_NODE + "/" + PathConstants.PRODUCTS_NODE));
        Document productsDocument = new Document();
        if (c.exists(productsPath)) {
            productsDocument = c.get(productsPath);
        } else {
            productsDocument.setData("");
            log.warn("Cannot find products path, using empty product list.");
        }
        String flags = updateFeatureFlags(spaceFlagsDocument, featureFlagDefinitionDocument, productsDocument,
                tenantId);

        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, flags);
        return spaceInfo;
    }

    public static CustomerSpaceInfo getInfoInCache(String contractId, String tenantId, String spaceId, TreeCache cache) throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId, spaceId);
        Camille c = CamilleEnvironment.getCamille();

        Path spacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                spaceId);
        Document spacePropertiesDocument = c.getInCache(spacePath.append(PathConstants.PROPERTIES_FILE), cache);
        CustomerSpaceProperties properties = DocumentUtils.toTypesafeDocument(spacePropertiesDocument,
                CustomerSpaceProperties.class);

        Document spaceFlagsDocument = c.getInCache(spacePath.append(PathConstants.FEATURE_FLAGS_FILE), cache);
        Document featureFlagDefinitionDocument = c.getInCache(buildFeatureFlagDefinitionPath(), cache);
        Path productsPath = PathBuilder
                .buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)
                .append(new Path("/" + PathConstants.SPACECONFIGURATION_NODE + "/" + PathConstants.PRODUCTS_NODE));
        Document productsDocument = new Document();
        if (c.exists(productsPath)) {
            productsDocument = c.getInCache(productsPath, cache);
        } else {
            productsDocument.setData("");
            log.warn("Cannot find products path, using empty product list.");
        }
        String flags = updateFeatureFlags(spaceFlagsDocument, featureFlagDefinitionDocument, productsDocument,
                tenantId);

        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, flags);
        return spaceInfo;
    }

    public static Path buildFeatureFlagDefinitionPath() throws IllegalArgumentException, Exception {
        PathTranslator translator = PathTranslatorFactory.getTranslator(new PodDivisionScope());
        return translator.getAbsolutePath(new Path("/" + PathConstants.FEATURE_FLAGS_DEFINITIONS_FILE));
    }

    public static String updateFeatureFlags(Document tenantFeatureFlagDoc, Document featureFlagDefinitionDoc,
            Document productsDoc, String tenantId) throws JsonProcessingException, IOException {

        boolean orignalFeatureFlagIsEmpty = false;
        String tenantFeatureFlags = tenantFeatureFlagDoc.getData();
        if (StringUtils.isEmpty(tenantFeatureFlags)) {
            orignalFeatureFlagIsEmpty = true;
            log.info(String.format("Tenant %s original feature flags is empty", tenantId));
        } else {
            log.info(String.format("existing feature flag is %s", tenantFeatureFlags));
        }
        String featureFlagDefinitions = featureFlagDefinitionDoc.getData();
        if (StringUtils.isEmpty(featureFlagDefinitions)) {
            throw new RuntimeException("featureFlagDefinitions is empty.");
        } else {
            log.info(String.format("featureFlagDefinitions is %s", featureFlagDefinitions));
        }
        String products = productsDoc.getData();
        if (StringUtils.isEmpty(products)) {
            log.info(String.format("Tenant %s does not have products information", tenantId));
            return tenantFeatureFlagDoc.getData();
        }

        Set<String> productFeatureFlagSet = new HashSet<String>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode featureFlagDefinitionsNode = mapper.readTree(featureFlagDefinitions);
        ArrayNode productsForTenant = (ArrayNode) mapper.readTree(products);
        Iterator<Entry<String, JsonNode>> featureFlagDefinitionIter = featureFlagDefinitionsNode.fields();
        while (featureFlagDefinitionIter.hasNext()) {
            Entry<String, JsonNode> featureFlagDefinition = featureFlagDefinitionIter.next();
            JsonNode productsForFeatureFlag = featureFlagDefinition.getValue().get("AvailableProducts");
            if (productsForFeatureFlag.isNull()) {
                log.warn(String.format("featureFlagDefinition %s does not have AvailableProducts",
                        featureFlagDefinition.getValue().get("DisplayName").asText()));
                continue;
            }
            if (overlappingProductsExists((ArrayNode) productsForFeatureFlag, productsForTenant)) {
                productFeatureFlagSet.add(featureFlagDefinition.getValue().get("DisplayName").asText());
            }
        }

        JsonNode featureFlags = null;
        if (!orignalFeatureFlagIsEmpty) {
            featureFlags = mapper.readTree(tenantFeatureFlags);
        }
        JsonNode updatedFeatureFlag = mapper.createObjectNode();
        for (String selectedFeatureFlag : productFeatureFlagSet) {
            boolean matchedValue = false;
            if (featureFlags != null) {
                Iterator<Entry<String, JsonNode>> featureFlagIter = featureFlags.fields();
                while (featureFlagIter.hasNext()) {
                    Entry<String, JsonNode> featureFlag = featureFlagIter.next();
                    if (selectedFeatureFlag.equals(featureFlag.getKey())) {
                        matchedValue = featureFlag.getValue().asBoolean();
                        break;
                    }
                }
            }
            ((ObjectNode) updatedFeatureFlag).put(selectedFeatureFlag, matchedValue);
        }

        return updatedFeatureFlag.toString();
    }

    public static boolean overlappingProductsExists(ArrayNode productsForFeatureFlag, ArrayNode productsForTenant) {
        if (productsForFeatureFlag == null || productsForTenant == null) {
            return false;
        }
        for (JsonNode productForFeatureFlag : productsForFeatureFlag) {
            for (JsonNode productForTenant : productsForTenant) {
                if (productForFeatureFlag.asText().equals(productForTenant.asText())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static List<AbstractMap.SimpleEntry<String, CustomerSpaceInfo>> getAll(String contractId, String tenantId)
            throws Exception {
        LifecycleUtils.validateIds(contractId, tenantId);

        List<AbstractMap.SimpleEntry<String, CustomerSpaceInfo>> toReturn = new ArrayList<AbstractMap.SimpleEntry<String, CustomerSpaceInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<AbstractMap.SimpleEntry<Document, Path>> childPairs = c
                .getChildren(PathBuilder.buildCustomerSpacesPath(CamilleEnvironment.getPodId(), contractId, tenantId));

        for (AbstractMap.SimpleEntry<Document, Path> childPair : childPairs) {
            String spaceId = "";
            try {
                spaceId = childPair.getValue().getSuffix();
                toReturn.add(new AbstractMap.SimpleEntry<String, CustomerSpaceInfo>(spaceId,
                        getInfo(contractId, tenantId, spaceId)));
            } catch (Exception ex) {
                log.warn("Failed to add spaceId=" + spaceId);
            }
        }

        return toReturn;
    }

    public static List<AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>> getAll() throws Exception {
        List<AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>> toReturn = new ArrayList<AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<AbstractMap.SimpleEntry<Document, Path>> contracts = c
                .getChildren(PathBuilder.buildContractsPath(CamilleEnvironment.getPodId()));
        for (AbstractMap.SimpleEntry<Document, Path> contract : contracts) {
            String contractId = contract.getValue().getSuffix();
            List<AbstractMap.SimpleEntry<Document, Path>> tenants = c
                    .getChildren(PathBuilder.buildTenantsPath(CamilleEnvironment.getPodId(), contractId));
            for (AbstractMap.SimpleEntry<Document, Path> tenant : tenants) {
                String tenantId = tenant.getValue().getSuffix();
                List<AbstractMap.SimpleEntry<Document, Path>> spaces = c.getChildren(
                        PathBuilder.buildCustomerSpacesPath(CamilleEnvironment.getPodId(), contractId, tenantId));

                for (AbstractMap.SimpleEntry<Document, Path> space : spaces) {
                    String spaceId = "";
                    try {
                        spaceId = space.getValue().getSuffix();
                        toReturn.add(new AbstractMap.SimpleEntry<CustomerSpace, CustomerSpaceInfo>(
                                new CustomerSpace(contractId, tenantId, spaceId),
                                getInfo(contractId, tenantId, spaceId)));
                    } catch (Exception ex) {
                        log.warn("Failed to add spaceId=" + spaceId);
                    }
                }
            }
        }

        return toReturn;
    }
}
