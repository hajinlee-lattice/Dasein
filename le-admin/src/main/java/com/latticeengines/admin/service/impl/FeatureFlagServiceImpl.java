package com.latticeengines.admin.service.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.FeatureFlagService;
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

    @Override
    public void defineFlag(String id, FeatureFlagDefinition definition) {
        if (FeatureFlagClient.getDefinition(id) != null) {
            throw new LedpException(LedpCode.LEDP_19106, new RuntimeException(String.format(
                    "The definition of %s already exists.", id)));
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
            FeatureFlagValueMap flags = FeatureFlagClient.getFlags(space);

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

    @PostConstruct
    void defineDefaultFeatureFlags() {
        Set<LatticeProduct> danteProdSet = new HashSet<LatticeProduct>();
        danteProdSet.add(LatticeProduct.LPA);
        Set<LatticeProduct> quotaProdSet = new HashSet<LatticeProduct>();
        quotaProdSet.add(LatticeProduct.PD);
        Set<LatticeProduct> targetMarketProdSet = new HashSet<LatticeProduct>();
        targetMarketProdSet.add(LatticeProduct.PD);
        Set<LatticeProduct> verifySourceCredentialProdSet = new HashSet<LatticeProduct>();
        verifySourceCredentialProdSet.add(LatticeProduct.PD);
        Set<LatticeProduct> enablePocTransformProdSet = new HashSet<LatticeProduct>();
        enablePocTransformProdSet.add(LatticeProduct.LPA3);
        Set<LatticeProduct> useSalesforceSettingsProdSet = new HashSet<LatticeProduct>();
        useSalesforceSettingsProdSet.add(LatticeProduct.LPA3);
        Set<LatticeProduct> useMarketoSettingsProdSet = new HashSet<LatticeProduct>();
        useMarketoSettingsProdSet.add(LatticeProduct.LPA3);
        Set<LatticeProduct> useEloquaSettingsProdSet = new HashSet<LatticeProduct>();
        useEloquaSettingsProdSet.add(LatticeProduct.LPA3);
        Set<LatticeProduct> allowPivotFileProdSet = new HashSet<LatticeProduct>();
        allowPivotFileProdSet.add(LatticeProduct.LPA3);

        FeatureFlagDefinition danteFeatureFlag = createDefaultFeatureFlag(LatticeFeatureFlag.DANTE.getName(),
                LatticeFeatureFlag.DANTE.getDocumentation(), danteProdSet, true);
        FeatureFlagDefinition quotaFeatureFlag = createDefaultFeatureFlag(LatticeFeatureFlag.QUOTA.getName(),
                LatticeFeatureFlag.QUOTA.getDocumentation(), quotaProdSet, false);
        FeatureFlagDefinition targetMarketFeatureFlag = createDefaultFeatureFlag(
                LatticeFeatureFlag.TARGET_MARKET.getName(), LatticeFeatureFlag.TARGET_MARKET.getDocumentation(),
                targetMarketProdSet, false);
        FeatureFlagDefinition verifySourceCredentialFeatureFlag = createDefaultFeatureFlag(
                LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(),
                LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getDocumentation(), verifySourceCredentialProdSet, false);
        FeatureFlagDefinition enablePocTransformFeatureFlag = createDefaultFeatureFlag(
                LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName(),
                LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getDocumentation(), enablePocTransformProdSet, true);
        FeatureFlagDefinition useSalesforceSettingsFeatureFlag = createDefaultFeatureFlag(
                LatticeFeatureFlag.USE_SALESFORCE_SETTINGS.getName(),
                LatticeFeatureFlag.USE_SALESFORCE_SETTINGS.getDocumentation(), useMarketoSettingsProdSet, true);
        FeatureFlagDefinition useMarketoSettingsFeatureFlag = createDefaultFeatureFlag(
                LatticeFeatureFlag.USE_MARKETO_SETTINGS.getName(),
                LatticeFeatureFlag.USE_MARKETO_SETTINGS.getDocumentation(), useMarketoSettingsProdSet, true);
        FeatureFlagDefinition useEloquaSettingsFeatureFlag = createDefaultFeatureFlag(
                LatticeFeatureFlag.USE_ELOQUA_SETTINGS.getName(),
                LatticeFeatureFlag.USE_ELOQUA_SETTINGS.getDocumentation(), useEloquaSettingsProdSet, true);
        FeatureFlagDefinition allowPivotFileProdSetFeatureFlag = createDefaultFeatureFlag(
                LatticeFeatureFlag.ALLOW_PIVOT_FILE.getName(), LatticeFeatureFlag.ALLOW_PIVOT_FILE.getDocumentation(),
                allowPivotFileProdSet, true);

        FeatureFlagClient.setDefinition(LatticeFeatureFlag.DANTE.getName(), danteFeatureFlag);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.QUOTA.getName(), quotaFeatureFlag);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.TARGET_MARKET.getName(), targetMarketFeatureFlag);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(),
                verifySourceCredentialFeatureFlag);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName(),
                enablePocTransformFeatureFlag);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_SALESFORCE_SETTINGS.getName(),
                useSalesforceSettingsFeatureFlag);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_MARKETO_SETTINGS.getName(),
                useMarketoSettingsFeatureFlag);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_ELOQUA_SETTINGS.getName(), useEloquaSettingsFeatureFlag);
        FeatureFlagClient
                .setDefinition(LatticeFeatureFlag.ALLOW_PIVOT_FILE.getName(), allowPivotFileProdSetFeatureFlag);
    }

    private FeatureFlagDefinition createDefaultFeatureFlag(String displayName, String documentation,
            Set<LatticeProduct> latticeProduct, boolean configurable) {
        FeatureFlagDefinition featureFlagDef = new FeatureFlagDefinition();
        featureFlagDef.setDisplayName(displayName);
        featureFlagDef.setDocumentation(documentation);
        Set<LatticeProduct> featureFlagProdSet = new HashSet<LatticeProduct>();
        featureFlagProdSet.addAll(latticeProduct);
        featureFlagDef.setAvailableProducts(featureFlagProdSet);
        featureFlagDef.setConfigurable(configurable);
        return featureFlagDef;
    }

}
