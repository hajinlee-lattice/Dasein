package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.proxy.exposed.cdl.CDLDependenciesProxy;

@Component("cdlImpactValidator")
public class CDLImpactValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(CDLImpactValidator.class);

    @Inject
    private CDLDependenciesProxy cdlDependenciesProxy;

    public static final String VALIDATOR_NAME = "CDL_IMPACT_VALIDATOR";

    protected CDLImpactValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs, boolean isAdmin) {
        for (AttrConfig attrConfig : attrConfigs) {
            checkImpact(attrConfig, isAdmin);
        }
    }

    private void checkImpact(AttrConfig attrConfig, boolean isAdmin) {
        if (attrConfig.getEntity() != null && hasCustomValue(attrConfig)) {
            List<String> attributes = Collections.singletonList(String.format("%s.%s", attrConfig.getEntity().name(),
                    attrConfig.getAttrName()));
            if (MultiTenantContext.getCustomerSpace() == null) {
                log.error("MultiTenancy Framework error. Null CustomerSpace!");
                return;
            }
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            List<MetadataSegment> impactSegments = cdlDependenciesProxy.getDependingSegments(customerSpace, attributes);
            List<RatingEngine> impactRatingEngines = cdlDependenciesProxy.getDependingRatingEngines(customerSpace, attributes);
            List<RatingModel> impactRatingModels = cdlDependenciesProxy.getDependingRatingModels(customerSpace, attributes);
            List<Play> impactPlays = cdlDependenciesProxy.getDependingPlays(customerSpace, attributes);
            AttrConfigProp stateProp = attrConfig.getProperty(ColumnMetadataKey.State);
            if (CollectionUtils.isNotEmpty(impactSegments)) {
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    addErrorMsg(ValidationErrors.Type.IMPACTED_SEGMENTS,
                            String.format(ValidationMsg.Errors.IMPACT_SEGMENTS, attrConfig.getAttrName(),
                                    getImpactSegmentNames(impactSegments)),
                            attrConfig);
                } else {
                    addWarningMsg(ImpactWarnings.Type.IMPACTED_SEGMENTS,
                            String.format(ValidationMsg.Warnings.IMPACT_SEGMENTS, attrConfig.getAttrName(),
                                    getImpactSegmentNames(impactSegments)),
                            attrConfig);
                }
            }
            if (CollectionUtils.isNotEmpty(impactRatingEngines)) {
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    addErrorMsg(ValidationErrors.Type.IMPACTED_RATING_ENGINES,
                            String.format(ValidationMsg.Errors.IMPACT_RATING_ENGINES, attrConfig.getAttrName(),
                                    getImpactRatingEngineNames(impactRatingEngines)),
                            attrConfig);
                } else {
                    addWarningMsg(ImpactWarnings.Type.IMPACTED_RATING_ENGINES,
                            String.format(ValidationMsg.Warnings.IMPACT_RATING_ENGINES, attrConfig.getAttrName(),
                                    getImpactRatingEngineNames(impactRatingEngines)),
                            attrConfig);
                }
            }
            if (CollectionUtils.isNotEmpty(impactRatingModels)) {
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    addErrorMsg(ValidationErrors.Type.IMPACTED_RATING_MODELS,
                            String.format(ValidationMsg.Errors.IMPACT_RATING_MODELS, attrConfig.getAttrName(),
                                    getImpactRatingModleNames(impactRatingModels)),
                            attrConfig);
                } else {
                    addWarningMsg(ImpactWarnings.Type.IMPACTED_RATING_MODELS,
                            String.format(ValidationMsg.Warnings.IMPACT_RATING_MODELS, attrConfig.getAttrName(),
                                    getImpactRatingModleNames(impactRatingModels)),
                            attrConfig);
                }
            }
            if (CollectionUtils.isNotEmpty(impactPlays)) {
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    addErrorMsg(ValidationErrors.Type.IMPACTED_PLAYS, String.format(ValidationMsg.Errors.IMPACT_PLAYS,
                            attrConfig.getAttrName(), getImpactPlayNames(impactPlays)),
                            attrConfig);
                } else {
                    addWarningMsg(ImpactWarnings.Type.IMPACTED_PLAYS, String.format(ValidationMsg.Warnings.IMPACT_PLAYS,
                            attrConfig.getAttrName(), getImpactPlayNames(impactPlays)), attrConfig);
                }
            }
        }


    }

    private String getImpactSegmentNames(List<MetadataSegment> impactSegments) {
        List<String> segmentNames = new ArrayList<>();
        impactSegments.forEach(segment -> segmentNames.add(segment.getDisplayName()));
        return String.join(",", segmentNames);
    }

    private String getImpactRatingEngineNames(List<RatingEngine> impactRatingEngines) {
        List<String> ratingEngineNames = new ArrayList<>();
        impactRatingEngines.forEach(ratingEngine -> ratingEngineNames.add(ratingEngine.getDisplayName()));
        return String.join(",", ratingEngineNames);
    }

    private String getImpactRatingModleNames(List<RatingModel> impactRatingModels) {
        List<String> ratingModelNames = new ArrayList<>();
        impactRatingModels.forEach(ratingModel -> ratingModelNames.add(ratingModel.getId()));
        return String.join(",", ratingModelNames);
    }

    private String getImpactPlayNames(List<Play> impactPlays) {
        List<String> playNames = new ArrayList<>();
        impactPlays.forEach(play -> playNames.add(play.getDisplayName()));
        return String.join(",", playNames);
    }

    private boolean hasCustomValue(AttrConfig attrConfig) {
        boolean res = false;
        if (attrConfig == null) {
            return res;
        }
        List<AttrConfigProp<?>> customProps = attrConfig.getAttrProps().values().stream()
                .filter(attrConfigProp -> attrConfigProp.getCustomValue() != null).collect(Collectors.toList());
        res = CollectionUtils.isNotEmpty(customProps);
        return res;
    }
}
