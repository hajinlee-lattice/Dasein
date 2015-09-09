package com.latticeengines.pls.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;
import com.latticeengines.pls.service.DefaultFeatureFlagProvider;

@Component("propertiesFileFeatureFlagProvider")
public class PropertiesFileFeatureFlagProvider implements DefaultFeatureFlagProvider {

    private static final Log log = LogFactory.getLog(PropertiesFileFeatureFlagProvider.class);
    private FeatureFlagValueMap flags = new FeatureFlagValueMap();
    private FeatureFlagDefinitionMap flagDefinitions = new FeatureFlagDefinitionMap();

    @Value("${ff.PlsTestFlag:None}")
    private String plsTestFlag;

    @Value("${ff.SetupPage:None}")
    private String setupPage;

    @Value("${ff.AdminAlertsTab:None}")
    private String adminAlertsTab;

    @Override
    public FeatureFlagValueMap getDefaultFlags() {
        flagDefinitions = FeatureFlagClient.getDefinitions();

        setFlagIfNotNull(PlsFeatureFlag.SETUP_PAGE.getName(), setupPage);
        setFlagIfNotNull(PlsFeatureFlag.ADMIN_ALERTS_TAB.getName(), adminAlertsTab);

        setFlagIfNotNull("PlsTestFlag", plsTestFlag);
        return new FeatureFlagValueMap(flags);
    }

    private void setFlagIfNotNull(String key, String value) {
        if (!flags.containsKey(key) && flagDefinitions.containsKey(key) && !"None".equals(value)) {
            flags.put(key, Boolean.valueOf(value));
        } else if (flags.containsKey(key) && !flagDefinitions.containsKey(key)) {
            flags.remove(key);
        }
    }
}
