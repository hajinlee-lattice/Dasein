package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileStepBase;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.DataCollectionStatusUtils;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(ProfileRating.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileRating extends ProfileStepBase<ProcessRatingStepConfiguration> {

    public static final Logger log = LoggerFactory.getLogger(ProfileRating.class);

    public static final String BEAN_NAME = "profileRating";

    private String statsTablePrefix;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Rating;
    }

    private void initializeConfiguration() {
        BusinessEntity entity = getEntity();
        customerSpace = configuration.getCustomerSpace();
        statsTablePrefix = entity.name() + "Stats";
    }

    @Override
    protected void onPostTransformationCompleted() {
        String statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
        updateStatusDateForRating();
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ProfileRatings");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        List<TransformationStepConfig> steps = new ArrayList<>();

        String pivotedTableName = getStringValueFromContext(PIVOTED_RATINGS_TABLE_NAME);

        int profileStep = 0;
        int bucketStep = 1;

        TransformationStepConfig profile = profile(pivotedTableName);
        TransformationStepConfig bucket = bucket(profileStep, pivotedTableName, null);
        TransformationStepConfig calc = calcStats(profileStep, bucketStep, statsTablePrefix, null);

        steps.add(profile);
        steps.add(bucket);
        steps.add(calc);

        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    private void updateStatusDateForRating() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        status = DataCollectionStatusUtils.updateTimeForCategoryChange(status, getLongValueFromContext(PA_TIMESTAMP),
                Category.RATING);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }
}
