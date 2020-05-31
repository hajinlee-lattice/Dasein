package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileStepBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.DataCollectionStatusUtils;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(ProfileRating.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileRating extends ProfileStepBase<ProcessRatingStepConfiguration> {

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
        List<ProfileParameters.Attribute> declaredAttrs = getDeclaredAttrs(pivotedTableName);
        int profileStep = 0;
        int bucketStep = 1;

        TransformationStepConfig profile = profile(pivotedTableName, declaredAttrs);
        TransformationStepConfig calc = calcStats(profileStep, pivotedTableName, statsTablePrefix);

        steps.add(profile);
        steps.add(calc);

        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    private List<ProfileParameters.Attribute> getDeclaredAttrs(String pivotedTableName) {
        List<String> ratingAttrs = getRatingAttrs(pivotedTableName);
        List<ProfileParameters.Attribute> pAttrs = ratingAttrs.stream().map(attr -> {
            CategoricalBucket catBkt = new CategoricalBucket();
            catBkt.setCategories(Arrays.asList("A", "B", "C", "D", "E", "F"));
            return new ProfileParameters.Attribute(attr, null, null, catBkt);
        }).collect(Collectors.toList());
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.AccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLCreatedTime.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLUpdatedTime.name()));
        return pAttrs;
    }

    private List<String> getRatingAttrs(String pivotedTableName) {
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace.toString(), pivotedTableName);
        if (CollectionUtils.isNotEmpty(cms)) {
            return cms.stream()
                    .filter(cm -> (StringUtils.isBlank(cm.getJavaClass()) || "String".equals(cm.getJavaClass()))
                            && cm.getAttrName().startsWith(RatingEngine.RATING_ENGINE_PREFIX + "_")) //
                    .map(ColumnMetadata::getAttrName) //
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private void updateStatusDateForRating() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        status = DataCollectionStatusUtils.updateTimeForCategoryChange(status, getLongValueFromContext(PA_TIMESTAMP),
                Category.RATING);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }
}
