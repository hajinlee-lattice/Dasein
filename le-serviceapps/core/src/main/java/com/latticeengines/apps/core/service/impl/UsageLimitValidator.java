package com.latticeengines.apps.core.service.impl;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("usageLimitValidator")
public class UsageLimitValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(UsageLimitValidator.class);

    public static final String VALIDATOR_NAME = "USAGE_LIMIT_VALIDATOR";

    private List<AttrConfig> dbConfigs;

    private List<String> usageLimitCheckList = Arrays.asList(ColumnSelection.Predefined.Enrichment.getName(),
            ColumnSelection.Predefined.CompanyProfile.getName());

    protected UsageLimitValidator() {
        super(VALIDATOR_NAME);
    }

    public void setDBConfigs(List<AttrConfig> existingConfigs) {
        this.dbConfigs = existingConfigs;
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs, boolean isAdmin) {
        String tenantId = MultiTenantContext.getShortTenantId();
        log.info("validate usage limit for tenant " + tenantId);
        LimitValidatorUtils.checkAmbiguityInFieldNames(attrConfigs);
        for (String usage : usageLimitCheckList) {
            checkForUsage(attrConfigs, usage, getLimit(usage));
        }
    }

    @VisibleForTesting
    int getLimit(String usage) {
        int result = 0;
        switch (ColumnSelection.Predefined.fromName(usage)) {
        case Enrichment:
            result = (int) AttrConfigUsageOverview.defaultExportLimit;
            break;
        case CompanyProfile:
            result = (int) AttrConfigUsageOverview.defaultCompanyProfileLimit;
            break;
        default:
            break;
        }
        return result;
    }

    private void checkForUsage(List<AttrConfig> attrConfigs, String usage, int limit) {
        List<AttrConfig> userSelectedEnabledConfigs = LimitValidatorUtils.returnPropertyConfigs(attrConfigs, usage,
                Boolean.TRUE);
        List<AttrConfig> userSelectedDisabledConfigs = LimitValidatorUtils.returnPropertyConfigs(attrConfigs, usage,
                Boolean.FALSE);

        List<AttrConfig> existingActiveConfigs = LimitValidatorUtils.returnPropertyConfigs(dbConfigs, usage,
                Boolean.TRUE);

        List<AttrConfig> activeConfigs = LimitValidatorUtils.generateUnionConfig(existingActiveConfigs,
                userSelectedEnabledConfigs);
        List<AttrConfig> inactiveConfigs = LimitValidatorUtils.generateInterceptionConfig(existingActiveConfigs,
                userSelectedDisabledConfigs);

        int totalEnableNumber = activeConfigs.size() - inactiveConfigs.size();
        if (limit < totalEnableNumber) {
            userSelectedEnabledConfigs.forEach(e -> {
                addErrorMsg(ValidationErrors.Type.EXCEED_USAGE_LIMIT,
                        String.format(ValidationMsg.Errors.EXCEED_USAGE_LIMIT, totalEnableNumber, usage, limit), e);
            });
        }

    }

}
