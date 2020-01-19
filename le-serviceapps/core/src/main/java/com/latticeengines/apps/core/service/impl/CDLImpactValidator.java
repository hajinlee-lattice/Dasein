package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.proxy.exposed.cdl.CDLDependenciesProxy;

@Component("cdlImpactValidator")
public class CDLImpactValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(CDLImpactValidator.class);

    @Inject
    private CDLDependenciesProxy cdlDependenciesProxy;

    private static ExecutorService workers;

    public static final String VALIDATOR_NAME = "CDL_IMPACT_VALIDATOR";

    protected CDLImpactValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
            AttrValidation validation) {
        log.info(String.format("start to validate CDL impact for tenant %s", MultiTenantContext.getShortTenantId()));
        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            checkImpact(attrConfig);
        }
    }

    private void checkImpact(AttrConfig attrConfig) {
        if (attrConfig.getEntity() != null && hasCustomValue(attrConfig)) {
            List<String> attributes = Collections
                    .singletonList(String.format("%s.%s", attrConfig.getEntity().name(), attrConfig.getAttrName()));
            if (MultiTenantContext.getCustomerSpace() == null) {
                log.error("MultiTenancy Framework error. Null CustomerSpace!");
                return;
            }
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            AttrState attrState = attrConfig.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class);
            // skip impact checking for inactive attributes
            if (!AttrState.Inactive.equals(attrState)) {
                List<Runnable> runnables = generateParallelJob(attrConfig, customerSpace, attributes);
                // fork join execution
                ThreadPoolUtils.runInParallel(getWorkers(), runnables, 10, 1);
            }
        }
    }

    private List<Runnable> generateParallelJob(AttrConfig attrConfig, String customerSpace, List<String> attributes) {
        List<Runnable> threads = new ArrayList<>();
        if (isToBeDisabledForSegment(attrConfig)) {
            Runnable segmentRunnable = () -> {
                List<MetadataSegment> impactSegments = cdlDependenciesProxy.getDependingSegments(customerSpace,
                        attributes);
                if (CollectionUtils.isNotEmpty(impactSegments)) {
                    impactSegments.forEach(segment -> addWarningMsg(ImpactWarnings.Type.IMPACTED_SEGMENTS,
                            segment.getDisplayName(), attrConfig));
                }
            };
            Runnable ratingEngineRunnable = () -> {
                List<RatingEngine> impactRatingEngines = cdlDependenciesProxy
                        .getDependingRatingEngines(customerSpace, attributes);

                if (CollectionUtils.isNotEmpty(impactRatingEngines)) {
                    impactRatingEngines.forEach(re -> addWarningMsg(ImpactWarnings.Type.IMPACTED_RATING_ENGINES,
                            re.getDisplayName(), attrConfig));
                }
            };
            threads.add(segmentRunnable);
            threads.add(ratingEngineRunnable);

        }
        if (isToBeDisabledForTalkingPoint(attrConfig)) {
            Runnable playRunnable = () -> {
                List<Play> impactPlays = cdlDependenciesProxy.getDependantPlays(customerSpace, attributes);
                if (CollectionUtils.isNotEmpty(impactPlays)) {
                    impactPlays.forEach(play -> addWarningMsg(ImpactWarnings.Type.IMPACTED_PLAYS,
                            play.getDisplayName(), attrConfig));
                }
            };
            threads.add(playRunnable);
        }
        return threads;
    }
    private boolean isToBeDisabledForSegment(AttrConfig attrConfig) {
        return !Boolean.TRUE
                .equals(attrConfig.getPropertyFinalValue(ColumnSelection.Predefined.Segment.name(), Boolean.class));
    }

    private boolean isToBeDisabledForTalkingPoint(AttrConfig attrConfig) {
        return !Boolean.TRUE.equals(
                attrConfig.getPropertyFinalValue(ColumnSelection.Predefined.TalkingPoint.name(), Boolean.class));
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

    private static ExecutorService getWorkers() {
        if (workers == null) {
            synchronized (AbstractAttrConfigService.class) {
                if (workers == null) {
                    workers = ThreadPoolUtils.getCachedThreadPool("attr-validator-svc");
                }
            }
        }
        return workers;
    }
}
