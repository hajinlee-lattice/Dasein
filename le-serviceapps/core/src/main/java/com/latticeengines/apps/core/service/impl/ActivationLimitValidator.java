package com.latticeengines.apps.core.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

/**
 * Validate if limit of activation is exceeded.
 * 
 */

@Component("activationLimitValidator")
public class ActivationLimitValidator extends AttrValidator {
    public static final String VALIDATOR_NAME = "ACTIVAITON_LIMIT_VALIDATOR";
    private static final Logger log = LoggerFactory.getLogger(ActivationLimitValidator.class);
    @Inject
    private ZKConfigService zkConfigService;

    protected ActivationLimitValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs) {
        log.info(String.format("start to validate limit activation for tenant %s",
                MultiTenantContext.getShortTenantId()));
        // make sure user selected attr don't have two same attribute
        LimitValidatorUtils.checkAmbiguityInFieldNames(userProvidedAttrConfigs);
        // split user selected configs into active and inactive, props always
        // are not empty after render method
        List<AttrConfig> userSelectedActiveConfigs = LimitValidatorUtils.returnPropertyConfigs(userProvidedAttrConfigs,
                ColumnMetadataKey.State, AttrState.Active);
        List<AttrConfig> userSelectedInactiveConfigs = LimitValidatorUtils
                .returnPropertyConfigs(userProvidedAttrConfigs, ColumnMetadataKey.State, AttrState.Inactive);

        List<AttrConfig> existingActiveConfigs = LimitValidatorUtils.returnPropertyConfigs(existingAttrConfigs,
                ColumnMetadataKey.State, AttrState.Active);

        // dedup, since the user selected configs has merged with the configs in
        // DB, no need to count same configs in DB
        List<AttrConfig> activeConfigs = LimitValidatorUtils.generateUnionConfig(existingActiveConfigs,
                userSelectedActiveConfigs);
        List<AttrConfig> inactiveConfigs = LimitValidatorUtils.generateInterceptionConfig(existingActiveConfigs,
                userSelectedInactiveConfigs);
        checkDataLicense(activeConfigs, inactiveConfigs, userSelectedActiveConfigs);
        checkSystemLimit(activeConfigs, inactiveConfigs, userSelectedActiveConfigs);
    }

    private void checkDataLicense(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs) {
        String tenantId = MultiTenantContext.getShortTenantId();
        for (DataLicense license : DataLicense.values()) {
            int limit = zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(tenantId,
                    license.getDataLicense());
            List<AttrConfig> premiumActiveConfigs = configs.stream()
                    .filter(entity -> (license.getDataLicense().equals(entity.getDataLicense())))
                    .collect(Collectors.toList());
            List<AttrConfig> premiumInactiveConfigs = userSelectedInactiveConfigs.stream()
                    .filter(entity -> (license.getDataLicense().equals(entity.getDataLicense())))
                    .collect(Collectors.toList());

            int userSelectedNumber = premiumActiveConfigs.size() - premiumInactiveConfigs.size();
            if (limit < userSelectedNumber) {
                userSelectedActiveConfigs.forEach(e -> {
                    if (license.getDataLicense().equals(e.getDataLicense())) {
                        addErrorMsg(ValidationErrors.Type.EXCEED_DATA_LICENSE,
                                String.format(ValidationMsg.Errors.EXCEED_LIMIT, userSelectedNumber,
                                        license.getDescription(), limit),
                                e);
                    }
                });
            }
        }
    }

    // check category limit
    private void checkSystemLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs) {
        checkDetailSystemLimit(configs, userSelectedInactiveConfigs, userSelectedActiveConfigs,
                Category.ACCOUNT_ATTRIBUTES, (int) AbstractAttrConfigService.DEFAULT_LIMIT);
        checkDetailSystemLimit(configs, userSelectedInactiveConfigs, userSelectedActiveConfigs,
                Category.CONTACT_ATTRIBUTES, (int) AbstractAttrConfigService.DEFAULT_LIMIT);
    }

    private void checkDetailSystemLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs, Category category, int limit) {
        List<AttrConfig> list = configs.stream()
                .filter(e -> category.equals(e.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class)))
                .collect(Collectors.toList());
        List<AttrConfig> inactiveList = userSelectedInactiveConfigs.stream()
                .filter(e -> category.equals(e.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class)))
                .collect(Collectors.toList());
        int number = list.size() - inactiveList.size();
        if (number > limit) {
            userSelectedActiveConfigs.forEach(config -> {
                if (category.equals(config.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class))) {
                    addErrorMsg(ValidationErrors.Type.EXCEED_SYSTEM_LIMIT,
                            String.format(ValidationMsg.Errors.EXCEED_LIMIT, number, category.name(), limit), config);
                }
            });
        }
    }

}
