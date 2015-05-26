package com.latticeengines.camille.exposed.featureflags;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.latticeengines.camille.exposed.config.cache.ConfigurationCache;
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
import com.latticeengines.domain.exposed.camille.scopes.PodScope;

public class CamilleFeatureFlagProvider implements FeatureFlagProvider {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public CamilleFeatureFlagProvider() {
        definitionCache = ConfigurationCache.construct(new PodScope(), new Path("/"
                + PathConstants.FEATURE_FLAGS_DEFINITIONS_FILE));
        valueCache = ConfigurationMultiCache.construct();
    }

    @Override
    public boolean isEnabled(CustomerSpace space, String id) {
        FeatureFlagValueMap flags = getFlags(space);
        if (flags == null) {
            log.warn(String.format("No feature flag value file defined for customer space %s", space));
            return false;
        }
        if (!flags.containsKey(id)) {
            return false;
        }

        // generate warning if no definition
        getDefinition(id);

        return flags.get(id).booleanValue();
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
        if (getDefinition(id) == null) {
            throw new RuntimeException(String.format(
                    "Feature flag %s cannot be toggled without a corresponding definition", id));
        }

        SafeUpserter upserter = new SafeUpserter();
        upserter.upsert(new CustomerSpaceScope(space), new Path("/" + PathConstants.FEATURE_FLAGS_VALUES_FILE),
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
    }

    @Override
    public void setDefinition(final String id, final FeatureFlagDefinition definition) {
        SafeUpserter upserter = new SafeUpserter();
        upserter.upsert(new PodScope(), new Path("/" + PathConstants.FEATURE_FLAGS_DEFINITIONS_FILE),
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
    }

    @Override
    public void remove(String id) {
        // TODO Auto-generated method stub

    }

    /**
     * TODO This is slow - in the future this should be cached and updated via a
     * cache listener.
     */
    private FeatureFlagDefinitionMap getDefinitions() {
        Document doc = definitionCache.get();
        return DocumentUtils.toTypesafeDocument(doc, FeatureFlagDefinitionMap.class);
    }

    /**
     * TODO This is slow - in the future this should be cached and updated via a
     * cache listener.
     */
    private FeatureFlagValueMap getFlags(CustomerSpace space) {
        return valueCache.get(new CustomerSpaceScope(space), new Path("/" + PathConstants.FEATURE_FLAGS_VALUES_FILE),
                FeatureFlagValueMap.class);
    }

    private ConfigurationCache<PodScope> definitionCache;
    private ConfigurationMultiCache<CustomerSpaceScope> valueCache;
}
