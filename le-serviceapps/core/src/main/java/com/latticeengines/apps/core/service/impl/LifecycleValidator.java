package com.latticeengines.apps.core.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("lifecycleValidator")
public class LifecycleValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(LifecycleValidator.class);

    static final String VALIDATOR_NAME = "LIFECYCLE_VALIDATOR";

    @Inject
    private ApplicationContext applicationContext;

    protected LifecycleValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
                         AttrValidation validation) {
        log.info(String.format("start to validate lifecycle for tenant %s", MultiTenantContext.getShortTenantId()));
        Map<Category, Set<String>> categoryMap = buildCategoryMap();
        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            checkState(attrConfig, categoryMap);
        }
    }

    private Map<Category, Set<String>> buildCategoryMap() {
        try {
            AttrConfigService attrConfigService = applicationContext.getBean(AttrConfigService.class);
            List<AttributeSet> attributeSets = attrConfigService.getAttributeSets(true);
            return CollectionUtils.isNotEmpty(attributeSets) ? attributeSets.stream()
                    .filter(attributeSet -> MapUtils.isNotEmpty(attributeSet.getAttributesMap()))
                    .flatMap(attributeSet -> attributeSet.getAttributesMap().entrySet().stream())
                    .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue()))
                    .collect(Collectors.toMap(entry -> Category.valueOf(entry.getKey()), entry -> entry.getValue(),
                            (Set<String> newValueList, Set<String> oldValueList) -> {
                                oldValueList.addAll(newValueList);
                                return oldValueList;
                            })) : new HashMap<>();
        } catch (Exception e) {
            log.warn("Unable to build category map: {}.", e.getMessage());
            return new HashMap<>();
        }
    }

    private void checkState(AttrConfig attrConfig, Map<Category, Set<String>> categoryMap) {
        Map<String, AttrConfigProp<?>> attrConfigPropMap = attrConfig.getAttrProps();
        if (MapUtils.isEmpty(attrConfigPropMap)) {
            return;
        }
        AttrConfigProp<?> stateProp = attrConfig.getProperty(ColumnMetadataKey.State);
        AttrState finalState = attrConfig.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class);
        if (stateProp != null) {
            if (stateProp.getCustomValue() != null) {
                AttrState customState = AttrState.valueOf(stateProp.getCustomValue().toString());
                // do not allow activation of deprecated attributes
                if (customState.equals(AttrState.Active) && Boolean.TRUE.equals(attrConfig.getShouldDeprecate())) {
                    addErrorMsg(ValidationErrors.Type.INVALID_ACTIVATION,
                            String.format(ValidationMsg.Errors.FORBID_SET_ACTIVE, attrConfig.getAttrName()),
                            attrConfig);
                }
            }
            // check customer value or system value is equal to Inactive
            if (AttrState.Inactive.equals(finalState)) {
                for (String group : ColumnSelection.Predefined.usageProperties) {
                    // PLS-10731 Activation Status does not apply in Modeling
                    if (!ColumnSelection.Predefined.Model.name().equals(group)) {
                        Boolean finalUsageValue = attrConfig.getPropertyFinalValue(group, Boolean.class);
                        if (Boolean.TRUE.equals((finalUsageValue))) {
                            addWarningMsg(ImpactWarnings.Type.USAGE_ENABLED, group, attrConfig);
                        }
                        if (ColumnSelection.Predefined.Enrichment.getName().equals(group)) {
                            // we need to check attribute usage in attribute set
                            Category category = attrConfig.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class);
                            if (category != null) {
                                Set<String> attributes = categoryMap.get(category);
                                if (CollectionUtils.isNotEmpty(attributes) && attributes.contains(attrConfig.getAttrName())) {
                                    addWarningMsg(ImpactWarnings.Type.USAGE_ENABLED, group, attrConfig);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
