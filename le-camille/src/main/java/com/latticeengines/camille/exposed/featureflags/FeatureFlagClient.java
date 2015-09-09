package com.latticeengines.camille.exposed.featureflags;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagProvider;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

public class FeatureFlagClient {

    public static void setProvider(FeatureFlagProvider provider) {
        if (provider == null) {
            throw new NullPointerException("Cannot set the FeatureFlagClient to use a null provider");
        }
        FeatureFlagClient.provider = provider;
    }

    public static boolean isEnabled(CustomerSpace space, String id) {
        initialize();
        return provider.isEnabled(space, id);
    }

    public static FeatureFlagDefinition getDefinition(String id) {
        initialize();
        return provider.getDefinition(id);
    }

    public static FeatureFlagDefinitionMap getDefinitions() {
        initialize();
        return provider.getDefinitions();
    }

    public static FeatureFlagValueMap getFlags(CustomerSpace space) {
        initialize();
        return provider.getFlags(space);
    }

    public static void setEnabled(CustomerSpace space, String id, boolean enabled) {
        initialize();
        provider.setEnabled(space, id, enabled);
    }

    public static void setDefinition(String id, FeatureFlagDefinition definition) {
        initialize();
        provider.setDefinition(id, definition);
    }

    public static void remove(String id) {
        initialize();
        provider.remove(id);
    }

    public static void removeFromSpace(CustomerSpace space, String id) {
        initialize();
        provider.removeFlagFromSpace(space, id);
    }

    private static void initialize() {
        if (provider == null) {
            synchronized (FeatureFlagClient.class) {
                if (provider == null) {
                    FeatureFlagClient.provider = new CamilleFeatureFlagProvider();
                }
            }
        }
    }

    private static FeatureFlagProvider provider;
}
