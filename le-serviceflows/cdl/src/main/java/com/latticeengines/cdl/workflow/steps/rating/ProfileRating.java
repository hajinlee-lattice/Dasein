package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.ProfileStepBase;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component(ProfileRating.BEAN_NAME)
public class ProfileRating extends ProfileStepBase<ProcessRatingStepConfiguration> {

    public static final String BEAN_NAME = "profileRating";

    private String ratingTablePrefix;
    private String statsTablePrefix;
    private String masterTableName;
    private List<RatingModelContainer> modelContainers;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Rating;
    }

    private void initializeConfiguration() {
        BusinessEntity entity = getEntity();
        customerSpace = configuration.getCustomerSpace();
        ratingTablePrefix = TableRoleInCollection.Rating.name();
        statsTablePrefix = entity.name() + "Stats";

        masterTableName = getObjectFromContext(RAW_RATING_TABLE_NAME, String.class);
        Table rawTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
        if (rawTable == null) {
            throw new IllegalStateException("Cannot find raw rating table " + masterTableName);
        }

        modelContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
    }

    @Override
    protected void onPostTransformationCompleted() {
        String ratingTableName = TableUtils.getFullTableName(ratingTablePrefix, pipelineVersion);
        String statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);

        Table servingStoreTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                ratingTableName);
        enrichTableSchema(servingStoreTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), ratingTableName, servingStoreTable);

//        updateEntityValueMapInContext(BusinessEntity.Rating, TABLE_GOING_TO_REDSHIFT, ratingTableName, String.class);
//        updateEntityValueMapInContext(BusinessEntity.Rating, APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);

        updateEntityValueMapInContext(SERVING_STORE_IN_STATS, ratingTableName, String.class);
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);

        metadataProxy.deleteTable(customerSpace.toString(), masterTableName);
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

        int profileStep = 0;
        int bucketStep = 1;

        String sortKey = InterfaceName.AccountId.name();

        TransformationStepConfig profile = profile(masterTableName);
        TransformationStepConfig bucket = bucket(profileStep, masterTableName);
        TransformationStepConfig calc = calcStats(profileStep, bucketStep, statsTablePrefix, null);
        TransformationStepConfig sort = sort(bucketStep, ratingTablePrefix, sortKey, 200);

        steps.add(profile);
        steps.add(bucket);
        steps.add(calc);
        steps.add(sort);

        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    private void enrichTableSchema(Table table) {
        List<Attribute> attrs = table.getAttributes();
        attrs.forEach(attr -> {
            attr.setCategory(Category.RATING);
            attr.removeAllowedDisplayNames();
        });
    }

}
