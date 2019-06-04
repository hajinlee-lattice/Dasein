package com.latticeengines.apps.core.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSpecification;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("usageValidator")
public class UsageValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(UsageValidator.class);

    public static final String VALIDATOR_NAME = "USAGE_VALIDATOR";

    protected UsageValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
            AttrValidation validation) {
        log.info(String.format("start to validate usage for tenant %s", MultiTenantContext.getShortTenantId()));
        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            checkUsage(attrConfig);
        }
    }

    private void checkUsage(AttrConfig attrConfig) {
        AttrSpecification attrSpec = parseAttrSpecification(attrConfig);
        if (attrSpec == null) {
            log.warn(String.format("Cannot get Attribute Specification for attribute %s with Type %s, SubType %s",
                    attrConfig.getAttrName(), attrConfig.getAttrType().name(), attrConfig.getAttrSubType().name()));
            return;
        }
        for (ColumnSelection.Predefined group : ColumnSelection.Predefined.values()) {
            AttrConfigProp<?> groupUsageProp = attrConfig.getProperty(group.name());
            if (groupUsageProp != null && groupUsageProp.getCustomValue() != null) {
                switch (group) {
                case Segment:
                    if (!attrSpec.segmentationChange()) {
                        addErrorMsg(ValidationErrors.Type.INVALID_USAGE_CHANGE,
                                String.format(ValidationMsg.Errors.INVALID_ATTRIBUTE_USAGE_CHANGE, group.name(),
                                        attrSpec.getSpecification()),
                                attrConfig);
                    }
                    break;
                case Enrichment:
                    if (!attrSpec.enrichmentChange()) {
                        addErrorMsg(ValidationErrors.Type.INVALID_USAGE_CHANGE,
                                String.format(ValidationMsg.Errors.INVALID_ATTRIBUTE_USAGE_CHANGE, group.name(),
                                        attrSpec.getSpecification()),
                                attrConfig);
                    }
                    break;
                case CompanyProfile:
                    if (!attrSpec.companyProfileChange()) {
                        addErrorMsg(ValidationErrors.Type.INVALID_USAGE_CHANGE,
                                String.format(ValidationMsg.Errors.INVALID_ATTRIBUTE_USAGE_CHANGE, group.name(),
                                        attrSpec.getSpecification()),
                                attrConfig);
                    }
                    break;
                case TalkingPoint:
                    if (!attrSpec.talkingPointChange()) {
                        addErrorMsg(ValidationErrors.Type.INVALID_USAGE_CHANGE,
                                String.format(ValidationMsg.Errors.INVALID_ATTRIBUTE_USAGE_CHANGE, group.name(),
                                        attrSpec.getSpecification()),
                                attrConfig);
                    }
                    break;
                case Model:
                    if (!attrSpec.modelChange()) {
                        addErrorMsg(ValidationErrors.Type.INVALID_USAGE_CHANGE,
                                String.format(ValidationMsg.Errors.INVALID_ATTRIBUTE_USAGE_CHANGE, group.name(),
                                        attrSpec.getSpecification()),
                                attrConfig);
                    }
                    break;
                default:
                    return;
                }
            }
        }

    }

    private AttrSpecification parseAttrSpecification(AttrConfig attrConfig) {
        AttrType attrType = attrConfig.getAttrType();
        AttrSubType attrSubType = attrConfig.getAttrSubType();
        BusinessEntity entity = attrConfig.getEntity();
        AttrSpecification attrSpecification = AttrSpecification.getAttrSpecification(attrType, attrSubType, entity);
        if (attrSpecification == null) {
            log.error(String.format("Cannot resolve Type %s and SubType %s to AttrSpecification", attrType.name(),
                    attrSubType.name()));
        }
        return attrSpecification;
    }
}
