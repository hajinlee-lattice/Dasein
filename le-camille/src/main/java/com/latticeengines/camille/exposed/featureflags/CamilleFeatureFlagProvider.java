package com.latticeengines.camille.exposed.featureflags;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.cache.ConfigurationCache;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.util.ConfigurationMultiCache;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.camille.exposed.util.SafeUpserter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagProvider;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.camille.scopes.PodDivisionScope;

public class CamilleFeatureFlagProvider implements FeatureFlagProvider {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public CamilleFeatureFlagProvider() {
        ensurePodDivisionExists();

        definitionCache = ConfigurationCache.construct(new PodDivisionScope(),
                new Path("/" + PathConstants.FEATURE_FLAGS_DEFINITIONS_FILE));
        valueCache = ConfigurationMultiCache.construct();
    }

    private void ensurePodDivisionExists() {
        Camille c = CamilleEnvironment.getCamille();
        if (!StringUtils.isEmpty(CamilleEnvironment.getDivision())) {
            try {
                c.upsert(PathBuilder.buildPodDivisionPath(CamilleEnvironment.getPodId(),
                        CamilleEnvironment.getDivision()), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (Exception e) {
                log.error("Could not upsert pod division path", e);
            }
        } else {
            log.info("CamilleEnvironment division is empty");
        }
    }

    @Override
    public boolean isEnabled(CustomerSpace space, String id) {
        // generate warning if no definition
        FeatureFlagDefinition def = getDefinition(id);
        boolean defaultValue = def != null && def.getDefaultValue();
        if (def != null && def.isDeprecated()) {
            return defaultValue;
        } else {
            FeatureFlagValueMap flags = getFlags(space);
            if (flags == null) {
                log.warn(String.format(
                        "No feature flag value file defined for customer space %s. Using default value of %s: %s", space,
                        id, String.valueOf(defaultValue)));
                return defaultValue;
            }
            if (!flags.containsKey(id)) {
                return defaultValue;
            } else {
                return flags.get(id);
            }
        }
    }

    @Override
    public FeatureFlagDefinition getDefinition(String id) {
        FeatureFlagDefinitionMap definitions = getDefinitions();
        if (definitions == null) {
            log.warn(String.format("When requesting feature flag %s, no feature flag definition file found", id));
            return null;
        }
        FeatureFlagDefinition definition = definitions.get(id);
        if (definition == null) {
            log.warn(String.format("Definition missing for feature flag %s", id));
        }

        return definition;
    }

    @Override
    public void setEnabled(final CustomerSpace space, final String id, final boolean enabled) {
        FeatureFlagDefinition featureFlag = getDefinition(id);
        if (featureFlag == null) {
            throw new RuntimeException(
                    String.format("Feature flag %s cannot be toggled without a corresponding definition", id));
        }

        CustomerSpaceScope customerSpaceScope = new CustomerSpaceScope(space);
        Path featureFlagValueFilePath = new Path("/" + PathConstants.FEATURE_FLAGS_FILE);
        SafeUpserter upserter = new SafeUpserter();
        upserter.upsert(customerSpaceScope, featureFlagValueFilePath,
                new Function<FeatureFlagValueMap, FeatureFlagValueMap>() {
                    @Override
                    public FeatureFlagValueMap apply(FeatureFlagValueMap existing) {
                        FeatureFlagValueMap toReturn = new FeatureFlagValueMap();

                        if (existing != null) {
                            toReturn.putAll(existing);
                        }

                        toReturn.put(id, enabled);
                        return toReturn;
                    }
                }, FeatureFlagValueMap.class);
        rebuildValues(space);
    }

    @Override
    public void removeFlagFromSpace(CustomerSpace space, final String id) {
        SafeUpserter upserter = new SafeUpserter();
        upserter.upsert(new CustomerSpaceScope(space), new Path("/" + PathConstants.FEATURE_FLAGS_FILE),
                new Function<FeatureFlagValueMap, FeatureFlagValueMap>() {
                    @Override
                    public FeatureFlagValueMap apply(FeatureFlagValueMap existing) {
                        FeatureFlagValueMap toReturn = new FeatureFlagValueMap();

                        if (existing != null) {
                            toReturn.putAll(existing);
                        }

                        if (toReturn.containsKey(id)) {
                            toReturn.remove(id);
                        }

                        return toReturn;
                    }
                }, FeatureFlagValueMap.class);
        rebuildValues(space);
    }

    @Override
    public void setDefinition(final String id, final FeatureFlagDefinition definition) {
        log.info(String.format("Setting definition for Flag %s in Pod %s Division %s", id,
                CamilleEnvironment.getPodId(), CamilleEnvironment.getDivision()));
        SafeUpserter upserter = new SafeUpserter();
        upserter.upsert(new PodDivisionScope(), new Path("/" + PathConstants.FEATURE_FLAGS_DEFINITIONS_FILE),
                new Function<FeatureFlagDefinitionMap, FeatureFlagDefinitionMap>() {
                    @Override
                    public FeatureFlagDefinitionMap apply(FeatureFlagDefinitionMap existing) {
                        FeatureFlagDefinitionMap toReturn = new FeatureFlagDefinitionMap();

                        if (existing != null) {
                            toReturn.putAll(existing);
                        }

                        toReturn.put(id, definition);
                        return toReturn;
                    }
                }, FeatureFlagDefinitionMap.class);
        rebuildDefinitions();
    }

    @Override
    public void remove(final String id) {
        SafeUpserter upserter = new SafeUpserter();
        upserter.upsert(new PodDivisionScope(), new Path("/" + PathConstants.FEATURE_FLAGS_DEFINITIONS_FILE),
                existing -> {
                    FeatureFlagDefinitionMap toReturn = new FeatureFlagDefinitionMap();

                    if (existing != null) {
                        toReturn.putAll(existing);
                    }

                    if (toReturn.containsKey(id)) {
                        toReturn.remove(id);
                    }

                    return toReturn;
                }, FeatureFlagDefinitionMap.class);
        rebuildDefinitions();
    }

    private void rebuildValues(CustomerSpace space) {
        try {
            valueCache.rebuild(new CustomerSpaceScope(space), new Path("/" + PathConstants.FEATURE_FLAGS_FILE));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void rebuildDefinitions() {
        try {
            definitionCache.rebuild();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * TODO This is slow - in the future this should be cached and updated via a
     * cache listener.
     */
    @Override
    public FeatureFlagDefinitionMap getDefinitions() {
        Document doc;
        try {
            doc = definitionCache.get();
        } catch (Exception e) {
            log.error("Exception occurred attempting to retrieve feature flag definitions", e);
            doc = null;
        }

        if (doc != null) {
            return DocumentUtils.toTypesafeDocument(doc, FeatureFlagDefinitionMap.class);
        } else {
            return new FeatureFlagDefinitionMap();
        }
    }

    @Override
    public FeatureFlagValueMap getFlags(CustomerSpace space) {
        FeatureFlagValueMap toReturn;
        try {
            toReturn = valueCache.get(new CustomerSpaceScope(space), new Path("/" + PathConstants.FEATURE_FLAGS_FILE),
                    FeatureFlagValueMap.class);
        } catch (Exception e) {
            log.error("Exception occurred attempting to retrieve feature flags");
            toReturn = null;
        }
        if (toReturn == null) {
            log.info("Flag-value map in cache is empty, returning empty map.");
            toReturn = new FeatureFlagValueMap();
        }
        return toReturn;
    }

    private ConfigurationCache<PodDivisionScope> definitionCache;
    private ConfigurationMultiCache<CustomerSpaceScope> valueCache;
}
