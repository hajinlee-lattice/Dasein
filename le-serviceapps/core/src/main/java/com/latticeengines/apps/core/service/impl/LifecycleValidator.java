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
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
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
import com.latticeengines.domain.exposed.util.AttributeUtils;

@Component("lifecycleValidator")
public class LifecycleValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(LifecycleValidator.class);

    static final String VALIDATOR_NAME = "LIFECYCLE_VALIDATOR";

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private BatonService batonService;

    protected LifecycleValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs, AttrValidation validation) {
        log.info(String.format("start to validate lifecycle for tenant %s", MultiTenantContext.getShortTenantId()));
        Map<String, Map<Category, Set<String>>> attributeSetMap = buildAttributeSetMap();
        boolean configurableSegmentExport = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.CONFIGURABLE_SEGMENT_EXPORT);
        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            checkState(attrConfig, attributeSetMap, configurableSegmentExport);
        }
    }

    private Map<String, Map<Category, Set<String>>> buildAttributeSetMap() {
        try {
            AttrConfigService attrConfigService = applicationContext.getBean(AttrConfigService.class);
            List<AttributeSet> attributeSets = attrConfigService.getAttributeSets(true);
            return attributeSets.stream().filter(attributeSet -> MapUtils.isNotEmpty(attributeSet.getAttributesMap()))
                    .collect(Collectors.toMap(attributeSet -> attributeSet.getDisplayName(),
                            attributeSet -> attributeSet.getAttributesMap().entrySet().stream()
                                    .collect(Collectors.toMap(entry -> Category.valueOf(entry.getKey()), entry -> entry.getValue()))));
        } catch (Exception e) {
            log.warn("Unable to build category map: {}.", e.getMessage());
            return new HashMap<>();
        }
    }

    private void checkState(AttrConfig attrConfig, Map<String, Map<Category, Set<String>>> attributeSetMap, boolean configurableSegmentExport) {
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
                        if (ColumnSelection.Predefined.Enrichment.getName().equals(group)) {
                            if (configurableSegmentExport) {
                                if (Boolean.TRUE.equals((finalUsageValue))) {
                                    addWarningMsg(ImpactWarnings.Type.USAGE_ENABLED, AttributeUtils.DEFAULT_ATTRIBUTE_SET_DISPLAY_NAME, attrConfig);
                                }
                                // we need to check attribute usage in attribute set
                                Category category = attrConfig.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class);
                                if (category != null) {
                                    for (Map.Entry<String, Map<Category, Set<String>>> entry : attributeSetMap.entrySet()) {
                                        Map<Category, Set<String>> categoryMap = entry.getValue();
                                        Set<String> attributes = categoryMap.get(category);
                                        if (CollectionUtils.isNotEmpty(attributes) && attributes.contains(attrConfig.getAttrName())) {
                                            addWarningMsg(ImpactWarnings.Type.USAGE_ENABLED, entry.getKey(), attrConfig);
                                        }
                                    }
                                }
                            } else {
                                if (Boolean.TRUE.equals((finalUsageValue))) {
                                    addWarningMsg(ImpactWarnings.Type.USAGE_ENABLED, group, attrConfig);
                                }
                            }
                        } else {
                            if (Boolean.TRUE.equals((finalUsageValue))) {
                                addWarningMsg(ImpactWarnings.Type.USAGE_ENABLED, group, attrConfig);
                            }
                        }
                    }
                }
            }
        }
    }
}
