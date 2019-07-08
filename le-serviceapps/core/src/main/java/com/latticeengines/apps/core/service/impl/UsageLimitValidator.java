package com.latticeengines.apps.core.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("usageLimitValidator")
public class UsageLimitValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(UsageLimitValidator.class);

    public static final String VALIDATOR_NAME = "USAGE_LIMIT_VALIDATOR";

    @Inject
    private ZKConfigService zkConfigService;

    private List<String> usageLimitCheckList = Arrays.asList(ColumnSelection.Predefined.Enrichment.getName(),
            ColumnSelection.Predefined.CompanyProfile.getName());

    protected UsageLimitValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
            AttrValidation validation) {
        String tenantId = MultiTenantContext.getShortTenantId();
        log.info("validate usage limit for tenant " + tenantId);
        LimitValidatorUtils.checkAmbiguityInFieldNames(userProvidedAttrConfigs);
        for (String usage : usageLimitCheckList) {
            checkForUsage(existingAttrConfigs, userProvidedAttrConfigs, usage, getLimit(usage));
        }
    }

    @VisibleForTesting
    int getLimit(String usage) {
        int result = 0;
        switch (ColumnSelection.Predefined.fromName(usage)) {
        case Enrichment:
        case CompanyProfile:
            result = zkConfigService
                    .getMaxPremiumLeadEnrichmentAttributesByLicense(MultiTenantContext.getShortTenantId(), usage);
            break;
        default:
            break;
        }
        return result;
    }

    private void checkForUsage(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
            String usage, int limit) {
        List<AttrConfig> userSelectedEnabledConfigs = LimitValidatorUtils.returnPropertyConfigs(userProvidedAttrConfigs,
                usage, Boolean.TRUE);
        List<AttrConfig> userSelectedDisabledConfigs = LimitValidatorUtils
                .returnPropertyConfigs(userProvidedAttrConfigs, usage, Boolean.FALSE);
        log.info("user selected enabled configs " + userSelectedEnabledConfigs.size());
        log.info("user selected disabled configs " + userSelectedDisabledConfigs.size());

        List<AttrConfig> existingEnabledConfigs = LimitValidatorUtils.returnPropertyConfigs(existingAttrConfigs, usage,
                Boolean.TRUE);
        log.info("existing enabled configs " + existingEnabledConfigs.size());
        existingEnabledConfigs = LimitValidatorUtils.returnPropertyConfigs(existingEnabledConfigs,
                ColumnMetadataKey.State, AttrState.Active);

        List<AttrConfig> activeConfigs = LimitValidatorUtils.generateUnionConfig(existingEnabledConfigs,
                userSelectedEnabledConfigs);
        List<AttrConfig> inactiveConfigs = LimitValidatorUtils.generateInterceptionConfig(existingEnabledConfigs,
                userSelectedDisabledConfigs);
        log.info("activeConfigs " + activeConfigs.size());
        log.info("inactiveConfigs " + inactiveConfigs.size());

        int totalEnableNumber = activeConfigs.size() - inactiveConfigs.size();
        if (limit < totalEnableNumber) {
            userSelectedEnabledConfigs.forEach(e -> {
                addErrorMsg(ValidationErrors.Type.EXCEED_USAGE_LIMIT,
                        String.format(ValidationMsg.Errors.EXCEED_USAGE_LIMIT, totalEnableNumber, usage, limit), e);
            });
        }
    }

}
