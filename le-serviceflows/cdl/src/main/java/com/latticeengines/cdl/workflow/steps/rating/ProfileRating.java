package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PIVOT_RATINGS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileStepBase;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component(ProfileRating.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileRating extends ProfileStepBase<ProcessRatingStepConfiguration> {

    public static final Logger log = LoggerFactory.getLogger(ProfileRating.class);

    public static final String BEAN_NAME = "profileRating";

    private String ruleBaseRawRating;
    private String aiBaseRawRating;
    private String inactiveRating;
    private boolean hasRuleRating = false;
    private boolean hasAIRating = false;
    private int ruleSrcIdx = -1;
    private int aiSrcIdx = -1;
    private int inactiveSrcIdx = -1;

    private String ratingTablePrefix;
    private String statsTablePrefix;
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

        Table ruleRawTable = null;
        Table aiRawTable = null;

        ruleBaseRawRating = getStringValueFromContext(RULE_RAW_RATING_TABLE_NAME);
        if (StringUtils.isNotBlank(ruleBaseRawRating)) {
            ruleRawTable = metadataProxy.getTable(customerSpace.toString(), ruleBaseRawRating);
            if (ruleRawTable == null) {
                log.warn("Cannot find rule based raw rating table " + ruleBaseRawRating);
            } else {
                hasRuleRating = true;
            }
        }

        aiBaseRawRating = getStringValueFromContext(AI_RAW_RATING_TABLE_NAME);
        if (StringUtils.isNotBlank(aiBaseRawRating)) {
            aiRawTable = metadataProxy.getTable(customerSpace.toString(), aiBaseRawRating);
            if (aiRawTable == null) {
                log.warn("Cannot find AI based raw rating table " + aiBaseRawRating);
            } else {
                hasAIRating = true;
            }
        }

        if (aiRawTable == null && ruleRawTable == null) {
            throw new IllegalStateException("Cannot find any raw rating table");
        }

        inactiveRating = getStringValueFromContext(INACTIVE_RATINGS_TABLE_NAME);
        modelContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
    }

    @Override
    protected void onPostTransformationCompleted() {
        String customerSpace = configuration.getCustomerSpace().toString();
        String statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);
        String ratingTableName = TableUtils.getFullTableName(ratingTablePrefix, pipelineVersion);
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        Table servingStoreTable = metadataProxy.getTable(customerSpace, ratingTableName);
        enrichTableSchema(servingStoreTable);
        metadataProxy.updateTable(customerSpace, ratingTableName, servingStoreTable);

        ratingTableName = renameServingStoreTable(servingStoreTable);
        exportTableRoleToRedshift(ratingTableName, getEntity().getServingStore());
        dataCollectionProxy.upsertTable(customerSpace, ratingTableName, getEntity().getServingStore(), inactive);

        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);

        cleanupTemporaryTables();
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
        setRawRatingInputs(step);
        step.setTransformer(TRANSFORMER_PIVOT_RATINGS);
        PivotRatingsConfig conf = createPivotRatingsConfig();
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private void setRawRatingInputs(TransformationStepConfig step) {
        String ruleSourceName = "RuleBasedRawRating";
        SourceTable ruleSourceTable = new SourceTable(ruleBaseRawRating, customerSpace);
        String aiSourceName = "AIBasedRawRating";
        SourceTable aiSourceTable = new SourceTable(aiBaseRawRating, customerSpace);

        List<String> baseSources = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        if (hasAIRating) {
            baseSources.add(aiSourceName);
            baseTables.put(aiSourceName, aiSourceTable);
            aiSrcIdx = baseSources.size() - 1;
        }
        if (hasRuleRating) {
            baseSources.add(ruleSourceName);
            baseTables.put(ruleSourceName, ruleSourceTable);
            ruleSrcIdx = baseSources.size() - 1;
        }
        if (StringUtils.isNotBlank(inactiveRating)) {
            String inactiveSourceName = "InactiveRating";
            SourceTable inactiveSourceTable = new SourceTable(inactiveRating, customerSpace);
            baseSources.add(inactiveSourceName);
            baseTables.put(inactiveSourceName, inactiveSourceTable);
            inactiveSrcIdx = baseSources.size() - 1;
        }

        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);
    }

    private PivotRatingsConfig createPivotRatingsConfig() {
        PivotRatingsConfig config = new PivotRatingsConfig();
        Map<String, String> modelIdToEngineIdMap = new HashMap<>();
        List<String> evModelIds = new ArrayList<>();
        List<String> aiModelIds = new ArrayList<>();
        for (RatingModelContainer modelContainer : modelContainers) {
            String engineId = modelContainer.getEngineSummary().getId();
            String modelId = modelContainer.getModel().getId();
            modelIdToEngineIdMap.put(modelId, RatingEngine.toRatingAttrName(engineId));
            RatingEngineType ratingEngineType = modelContainer.getEngineSummary().getType();
            if (RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                    || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType)) {
                aiModelIds.add(modelId);
                AIModel aiModel = (AIModel) modelContainer.getModel();
                if (PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType())) {
                    evModelIds.add(modelId);
                }
            }
        }
        config.setEvModelIds(evModelIds);
        config.setAiModelIds(aiModelIds);
        config.setIdAttrsMap(modelIdToEngineIdMap);
        if (aiSrcIdx > -1) {
            config.setAiSourceIdx(aiSrcIdx);
        }
        if (ruleSrcIdx > -1) {
            config.setRuleSourceIdx(ruleSrcIdx);
        }
        if (inactiveSrcIdx > -1) {
            config.setInactiveSourceIdx(inactiveSrcIdx);
        }
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

    private void cleanupTemporaryTables() {
        String customerSpace = configuration.getCustomerSpace().toString();
        if (StringUtils.isNotBlank(ruleBaseRawRating)) {
            metadataProxy.deleteTable(customerSpace, ruleBaseRawRating);
        }
        if (StringUtils.isNotBlank(aiBaseRawRating)) {
            metadataProxy.deleteTable(customerSpace, aiBaseRawRating);
        }
        String filterTableName = getStringValueFromContext(FILTER_EVENT_TARGET_TABLE_NAME);
        if (StringUtils.isNotBlank(filterTableName)) {
            metadataProxy.deleteTable(customerSpace, filterTableName);
        }
        Table preMatchTable = getObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE, Table.class);
        if (preMatchTable != null) {
            metadataProxy.deleteTable(customerSpace, preMatchTable.getName());
        }
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        if (eventTable != null) {
            metadataProxy.deleteTable(customerSpace, eventTable.getName());
        }
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (StringUtils.isNotBlank(scoreResultTableName)) {
            metadataProxy.deleteTable(customerSpace, scoreResultTableName);
        }
    }

}
