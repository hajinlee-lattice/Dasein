package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PIVOT_RATINGS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileStepBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component(ProfileRating.BEAN_NAME)
public class ProfileRating extends ProfileStepBase<ProcessRatingStepConfiguration> {

    public static final Logger log = LoggerFactory.getLogger(ProfileRating.class);

    public static final String BEAN_NAME = "profileRating";
    private static final String ENGINE_ATTR_PREFIX = "engine_";

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
        ratingTablePrefix = TableRoleInCollection.PivotedRating.name();
        statsTablePrefix = entity.name() + "Stats";

        masterTableName = getStringValueFromContext(RAW_RATING_TABLE_NAME);
        Table rawTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
        if (rawTable == null) {
            throw new IllegalStateException("Cannot find raw rating table " + masterTableName);
        }

        modelContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
    }

    @Override
    protected void onPostTransformationCompleted() {
        String statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);
        String ratingTableName = TableUtils.getFullTableName(ratingTablePrefix, pipelineVersion);

        Table servingStoreTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                ratingTableName);
        enrichTableSchema(servingStoreTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), ratingTableName, servingStoreTable);

        updateEntityValueMapInContext(TABLE_GOING_TO_REDSHIFT, ratingTableName, String.class);
        updateEntityValueMapInContext(APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
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

        int pivotStep = 0;
        int sortStep = 1;
        int profileStep = 2;
        int bucketStep = 3;

        TransformationStepConfig pivot = pivot();
        TransformationStepConfig sort = sort(pivotStep, ratingTablePrefix, InterfaceName.AccountId.name(), 200);
        TransformationStepConfig profile = profile(sortStep);
        TransformationStepConfig bucket = bucket(profileStep, sortStep);
        TransformationStepConfig calc = calcStats(profileStep, bucketStep, statsTablePrefix, null);

        steps.add(pivot);
        steps.add(sort);
        steps.add(profile);
        steps.add(bucket);
        steps.add(calc);

        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    private TransformationStepConfig pivot() {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(masterTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER_PIVOT_RATINGS);
        PivotRatingsConfig conf = createPivotRatingsConfig();
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private PivotRatingsConfig createPivotRatingsConfig() {
        PivotRatingsConfig config = new PivotRatingsConfig();
        Map<String, String> modelIdToEngineIdMap = new HashMap<>();
        for (RatingModelContainer modelContainer : modelContainers) {
            String engineId = modelContainer.getEngineSummary().getId();
            String modelId = modelContainer.getModel().getId();
            modelIdToEngineIdMap.put(modelId, RatingEngine.toRatingAttrName(engineId));
        }
        config.setIdAttrsMap(modelIdToEngineIdMap);
        return config;
    }

    private void enrichTableSchema(Table table) {
        List<Attribute> attrs = table.getAttributes();
        attrs.forEach(attr -> {
            attr.setSubcategory("Other");
            attr.setDisplayName(attr.getName());
            attr.setCategory(Category.RATING);
            attr.removeAllowedDisplayNames();
        });
    }

}
