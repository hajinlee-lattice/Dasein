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
            List<String> attributes = Collections
                    .singletonList(String.format("%s.%s", attrConfig.getEntity().name(), attrConfig.getAttrName()));
            if (MultiTenantContext.getCustomerSpace() == null) {
                log.error("MultiTenancy Framework error. Null CustomerSpace!");
                return;
            }
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            List<MetadataSegment> impactSegments = cdlDependenciesProxy.getDependingSegments(customerSpace, attributes);
            List<RatingEngine> impactRatingEngines = cdlDependenciesProxy.getDependingRatingEngines(customerSpace,
                    attributes);
            List<RatingModel> impactRatingModels = cdlDependenciesProxy.getDependingRatingModels(customerSpace,
                    attributes);
            List<Play> impactPlays = cdlDependenciesProxy.getDependingPlays(customerSpace, attributes);
            AttrConfigProp<?> stateProp = attrConfig.getProperty(ColumnMetadataKey.State);
            if (CollectionUtils.isNotEmpty(impactSegments)) {
                List<String> names = getImpactSegmentNames(impactSegments);
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    names.forEach(name -> addErrorMsg(ValidationErrors.Type.IMPACTED_SEGMENTS, name, attrConfig));
                } else {
                    names.forEach(name -> addWarningMsg(ImpactWarnings.Type.IMPACTED_SEGMENTS, name, attrConfig));
                }
            }
            if (CollectionUtils.isNotEmpty(impactRatingEngines)) {
                List<String> names = getImpactRatingEngineNames(impactRatingEngines);
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    names.forEach(name -> addErrorMsg(ValidationErrors.Type.IMPACTED_RATING_ENGINES, name, attrConfig));
                } else {
                    names.forEach(name -> addWarningMsg(ImpactWarnings.Type.IMPACTED_RATING_ENGINES, name, attrConfig));
                }
            }
            if (CollectionUtils.isNotEmpty(impactRatingModels)) {
                List<String> names = getImpactRatingModleNames(impactRatingModels);
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    names.forEach(name -> addErrorMsg(ValidationErrors.Type.IMPACTED_RATING_MODELS, name, attrConfig));
                } else {
                    names.forEach(name -> addWarningMsg(ImpactWarnings.Type.IMPACTED_RATING_MODELS, name, attrConfig));
                }
            }
            if (CollectionUtils.isNotEmpty(impactPlays)) {
                List<String> names = getImpactPlayNames(impactPlays);
                if (isAdmin && AttrState.Inactive.equals(stateProp.getCustomValue())) {
                    names.forEach(name -> addErrorMsg(ValidationErrors.Type.IMPACTED_PLAYS, name, attrConfig));
                } else {
                    names.forEach(name -> addWarningMsg(ImpactWarnings.Type.IMPACTED_PLAYS, name, attrConfig));
                }
            }
        }

    }

    private List<String> getImpactSegmentNames(List<MetadataSegment> impactSegments) {
        List<String> segmentNames = new ArrayList<>();
        impactSegments.forEach(segment -> segmentNames.add(segment.getDisplayName()));
        return segmentNames;
    }

    private List<String> getImpactRatingEngineNames(List<RatingEngine> impactRatingEngines) {
        List<String> ratingEngineNames = new ArrayList<>();
        impactRatingEngines.forEach(ratingEngine -> ratingEngineNames.add(ratingEngine.getDisplayName()));
        return ratingEngineNames;
    }

    private List<String> getImpactRatingModleNames(List<RatingModel> impactRatingModels) {
        List<String> ratingModelNames = new ArrayList<>();
        impactRatingModels.forEach(ratingModel -> ratingModelNames.add(ratingModel.getId()));
        return ratingModelNames;
    }

    private List<String> getImpactPlayNames(List<Play> impactPlays) {
        List<String> playNames = new ArrayList<>();
        impactPlays.forEach(play -> playNames.add(play.getDisplayName()));
        return playNames;
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
