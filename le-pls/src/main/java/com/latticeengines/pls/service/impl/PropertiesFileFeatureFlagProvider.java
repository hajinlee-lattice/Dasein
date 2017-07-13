package com.latticeengines.pls.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;
import com.latticeengines.pls.service.DefaultFeatureFlagProvider;

@Component("propertiesFileFeatureFlagProvider")
public class PropertiesFileFeatureFlagProvider implements DefaultFeatureFlagProvider {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PropertiesFileFeatureFlagProvider.class);
    private FeatureFlagValueMap flags = new FeatureFlagValueMap();
    private FeatureFlagDefinitionMap flagDefinitions = new FeatureFlagDefinitionMap();

    @Value("${pls.ff.plstestflag:None}")
    private String plsTestFlag;

    @Value("${pls.ff.setuppage:None}")
    private String setupPage;

    @Value("${pls.ff.adminalertstab:None}")
    private String adminAlertsTab;

    @Value("${pls.ff.deploymentwizardpage:None}")
    private String deploymentWizardPage;

    @Value("${pls.ff.leadenrichmentpage:None}")
    private String leadEnrichmentPage;

    @Override
    public FeatureFlagValueMap getDefaultFlags() {
        flagDefinitions = FeatureFlagClient.getDefinitions();

        setFlagIfNotNull(PlsFeatureFlag.SETUP_PAGE.getName(), setupPage);
        setFlagIfNotNull(PlsFeatureFlag.ADMIN_ALERTS_TAB.getName(), adminAlertsTab);
        setFlagIfNotNull(PlsFeatureFlag.DEPLOYMENT_WIZARD_PAGE.getName(), deploymentWizardPage);
        setFlagIfNotNull(PlsFeatureFlag.LEAD_ENRICHMENT_PAGE.getName(), leadEnrichmentPage);

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
