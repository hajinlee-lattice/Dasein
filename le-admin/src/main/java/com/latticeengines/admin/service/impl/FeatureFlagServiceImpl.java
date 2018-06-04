package com.latticeengines.admin.service.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("featureFlagService")
public class FeatureFlagServiceImpl implements FeatureFlagService {

    private Map<LatticeFeatureFlag, FeatureFlagDefinition> flagDefinitionMap = new HashMap<>();

    @Value("${admin.overwrite.cdl.autoschedule:true}")
    private boolean autoSchedule;

    @Autowired
    private BatonService batonService;

    @Override
    public void defineFlag(String id, FeatureFlagDefinition definition) {
        if (FeatureFlagClient.getDefinition(id) != null) {
            throw new LedpException(LedpCode.LEDP_19106,
                    new RuntimeException(String.format("The definition of %s already exists.", id)));
        }
        FeatureFlagClient.setDefinition(id, definition);
    }

    @Override
    public void undefineFlag(String id) {
        FeatureFlagClient.remove(id);
    }

    @Override
    public FeatureFlagDefinition getDefinition(String id) {
        return FeatureFlagClient.getDefinition(id);
    }

    @Override
    public FeatureFlagDefinitionMap getDefinitions() {
        try {
            return FeatureFlagClient.getDefinitions();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19107, e);
        }
    }

    @Override
    public void setFlag(String tenantId, String flagId, boolean value) {
        try {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            FeatureFlagClient.setEnabled(space, flagId, value);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19108, e, new String[] { flagId, tenantId });
        }
    }

    @Override
    public void removeFlag(String tenantId, String flagId) {
        try {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            FeatureFlagClient.removeFromSpace(space, flagId);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19110, e, new String[] { flagId, tenantId });
        }
    }

    @Override
    public FeatureFlagValueMap getFlags(String tenantId) {
        try {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            FeatureFlagValueMap flags = batonService.getFeatureFlags(space);

            FeatureFlagValueMap toReturn = new FeatureFlagValueMap();
            if (flags == null) {
                return toReturn;
            }

            Set<String> definedFlags = getDefinitions().keySet();
            for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
                if (definedFlags.contains(flag.getKey())) {
                    toReturn.put(flag.getKey(), flag.getValue());
                }
            }

            return toReturn;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19109, e, new String[] { tenantId });
        }
    }

    @SuppressWarnings("deprecation")
    @PostConstruct
    void defineDefaultFeatureFlags() {
        // PD flags
        Collection<LatticeProduct> pd = Collections.singleton(LatticeProduct.PD);
        createDefaultFeatureFlag(LatticeFeatureFlag.QUOTA, pd);
        createDefaultFeatureFlag(LatticeFeatureFlag.TARGET_MARKET, pd);
        createDefaultFeatureFlag(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL, pd);

        // LPI flags
        Collection<LatticeProduct> lpi = Collections.singleton(LatticeProduct.LPA3);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_POC_TRANSFORM, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.USE_SALESFORCE_SETTINGS, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.USE_MARKETO_SETTINGS, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.USE_ELOQUA_SETTINGS, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.ALLOW_PIVOT_FILE, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_FUZZY_MATCH, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.LATTICE_INSIGHTS, lpi).setDefaultValue(true);

        createDefaultFeatureFlag(LatticeFeatureFlag.BYPASS_DNB_CACHE, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_LATTICE_MARKETO_CREDENTIAL_PAGE, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_MATCH_DEBUG, lpi);
        createDefaultFeatureFlag(LatticeFeatureFlag.VDB_MIGRATION, lpi);

        // CG flags
        Collection<LatticeProduct> cg = Collections.singleton(LatticeProduct.CG);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_CAMPAIGN_UI, cg);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_CDL, cg);
        createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_LPI_PLAYMAKER, cg).setDefaultValue(true);
        createDefaultFeatureFlag(LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE, cg).setDefaultValue(true);


        // multi-product flags
        FeatureFlagDefinition enableDataEncryption = createDefaultFeatureFlag(LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION,
                Arrays.asList(LatticeProduct.LPA3, LatticeProduct.CG));
        enableDataEncryption.setModifiableAfterProvisioning(false);
        enableDataEncryption.setDefaultValue(true);
        createDefaultFeatureFlag(LatticeFeatureFlag.DANTE, Arrays.asList(LatticeProduct.LPA, LatticeProduct.CG)) //
                .setDefaultValue(true);

        // register to feature flag client
        registerAllFlags();

        // overwrite default for deprecated flags
        // normally deprecated flag should always be true, but there are
        // exceptions
        overwriteDefaultValueForDeprecatedFlag(LatticeFeatureFlag.QUOTA, false);
        overwriteDefaultValueForDeprecatedFlag(LatticeFeatureFlag.TARGET_MARKET, false);
        overwriteDefaultValueForDeprecatedFlag(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL, false);
        overwriteDefaultValueForDeprecatedFlag(LatticeFeatureFlag.BYPASS_DNB_CACHE, false);
        overwriteDefaultValueForDeprecatedFlag(LatticeFeatureFlag.ENABLE_CAMPAIGN_UI, false);
        overwriteDefaultValue(LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE, true, autoSchedule);
    }

    private FeatureFlagDefinition createDefaultFeatureFlag(LatticeFeatureFlag featureFlag,
            Collection<LatticeProduct> latticeProducts) {
        FeatureFlagDefinition featureFlagDef = new FeatureFlagDefinition();
        featureFlagDef.setDisplayName(featureFlag.getName());
        featureFlagDef.setDocumentation(featureFlag.getDocumentation());
        Set<LatticeProduct> featureFlagProdSet = new HashSet<>(latticeProducts);
        featureFlagDef.setAvailableProducts(featureFlagProdSet);
        featureFlagDef.setConfigurable(true);
        featureFlagDef.setModifiableAfterProvisioning(true);
        featureFlagDef.setDefaultValue(featureFlag.isDeprecated());
        featureFlagDef.setDeprecated(featureFlag.isDeprecated());
        flagDefinitionMap.put(featureFlag, featureFlagDef);
        return featureFlagDef;
    }

    private void registerAllFlags() {
        for (Map.Entry<LatticeFeatureFlag, FeatureFlagDefinition> entry : flagDefinitionMap.entrySet()) {
            FeatureFlagDefinition def = entry.getValue();
            LatticeFeatureFlag flag = entry.getKey();
            if (flag.isDeprecated()) {
                def.setDefaultValue(true);
            }
            FeatureFlagClient.setDefinition(flag.getName(), def);
        }
    }

    private void overwriteDefaultValueForDeprecatedFlag(LatticeFeatureFlag flag, boolean defaultValue) {
        if (flag.isDeprecated()) {
            FeatureFlagDefinition def = flagDefinitionMap.get(flag);
            def.setDefaultValue(defaultValue);
            FeatureFlagClient.setDefinition(flag.getName(), def);
        }
    }

    private void overwriteDefaultValue(LatticeFeatureFlag flag, boolean defaultValue, boolean realValue) {
        if (defaultValue ^ realValue) {
            FeatureFlagDefinition def = flagDefinitionMap.get(flag);
            def.setDefaultValue(realValue);
            FeatureFlagClient.setDefinition(flag.getName(), def);
        }
    }
}
