package com.latticeengines.cdl.workflow.steps.rating;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Component("prepareForRating")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareForRating extends BaseWorkflowStep<ProcessRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareForRating.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    private String customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace().toString();
        List<RatingModelContainer> activeModels = readActiveRatingModels();
        log.info("Found " + CollectionUtils.size(activeModels) + " active rating models.");
        putObjectInContext(RATING_MODELS, activeModels);

        initializeRatingLifts();

        removeObjectFromContext(TABLES_GOING_TO_DYNAMO);
        removeObjectFromContext(TABLES_GOING_TO_REDSHIFT);
        removeObjectFromContext(STATS_TABLE_NAMES);
    }

    private List<RatingModelContainer> readActiveRatingModels() {
        List<RatingEngineSummary> summaries = ratingEngineProxy.getRatingEngineSummaries(customerSpace);
        if (CollectionUtils.isNotEmpty(summaries)) {
            List<RatingModelContainer> activeModels = Flux.fromIterable(summaries) //
                    .parallel().runOn(Schedulers.parallel()) //
                    .map(this::getValidRatingModel) //
                    .filter(container -> container.getModel() != null) //
                    .sequential().collectList().block();
            if (CollectionUtils.isNotEmpty(activeModels)) {
                List<String> modelGuids = activeModels.stream().filter(container -> {
                    RatingEngineType engineType = container.getEngineSummary().getType();
                    return RatingEngineType.CUSTOM_EVENT.equals(engineType)
                            || RatingEngineType.CROSS_SELL.equals(engineType);
                }).map(container -> {
                    AIModel aiModel = (AIModel) container.getModel();
                    return aiModel.getModelSummaryId();
                }).collect(Collectors.toList());
                putStringValueInContext(SCORING_MODEL_ID, StringUtils.join(modelGuids, "|"));
            }
            Set<String> existingEngineIds = new HashSet<>();
            DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
            Table table = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Rating.getServingStore(), active);
            if (table != null) {
                table.getAttributes().forEach(attr -> {
                    if (attr.getName().startsWith(RatingEngine.RATING_ENGINE_PREFIX)) {
                        String engineId = RatingEngine.toEngineId(attr.getName());
                        existingEngineIds.add(engineId);
                    }
                });
            }
            List<String> inactiveEngineIds = Flux.fromIterable(summaries) //
                    .filter(summary -> !RatingEngineStatus.ACTIVE.equals(summary.getStatus())
                            && !Boolean.TRUE.equals(summary.getDeleted())
                            && existingEngineIds.contains(summary.getId())) //
                    .map(RatingEngineSummary::getId) //
                    .collectList().block();
            log.info("Found " + CollectionUtils.size(inactiveEngineIds) + " inactive rating engines.");
            if (CollectionUtils.isNotEmpty(inactiveEngineIds)) {
                putObjectInContext(INACTIVE_ENGINES, inactiveEngineIds);
            }
            return activeModels;
        } else {
            log.info("There is no rating engine summaries");
            return null;
        }
    }

    private RatingModelContainer getValidRatingModel(RatingEngineSummary summary) {
        RatingModelContainer container = new RatingModelContainer(null, summary, null);
        String engineId = summary.getId();
        String engineName = summary.getDisplayName();
        RatingEngine engine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
        List<BucketMetadata> scoringBucketMetadata = null;
        if (isValidRatingEngine(engine)) {
            RatingModel ratingModel = engine.getScoringIteration();
            boolean isValid = false;
            if (RatingEngineType.CROSS_SELL.equals(summary.getType())) {
                AIModel aiModel = (AIModel) ratingModel;
                isValid = isValidCrossSellModel(aiModel);
                if (!isValid) {
                    log.warn("Cross sell rating model " + aiModel.getId() + " of " + engineId + " : " + engineName
                            + " is not ready for scoring.");
                } else {
                    scoringBucketMetadata = bucketedScoreProxy.getLatestABCDBucketsByModelGuid(customerSpace,
                            aiModel.getModelSummaryId());
                }
            } else if (RatingEngineType.CUSTOM_EVENT.equals(summary.getType())) {
                AIModel aiModel = (AIModel) ratingModel;
                isValid = isValidCustomEventModel(aiModel);
                if (!isValid) {
                    log.warn("Custom event rating model " + aiModel.getId() + " of " + engineId + " : " + engineName
                            + " is not ready for scoring.");
                } else {
                    scoringBucketMetadata = bucketedScoreProxy.getLatestABCDBucketsByModelGuid(customerSpace,
                            aiModel.getModelSummaryId());
                }
            } else if (RatingEngineType.RULE_BASED.equals(summary.getType())) {
                isValid = isValidRuleBasedModel((RuleBasedModel) ratingModel);
                if (!isValid) {
                    log.warn("Rule based rating model " + ratingModel.getId() + " of " + engineId + " : " + engineName
                            + " is not ready for scoring.");
                } else {
                    scoringBucketMetadata = summary.getBucketMetadata();
                }
            }

            if (CollectionUtils.isEmpty(scoringBucketMetadata)) {
                scoringBucketMetadata = BucketMetadataUtils.getDefaultMetadata();
            }

            if (isValid) {
                container = new RatingModelContainer(ratingModel, summary, scoringBucketMetadata);
            }
        }
        return container;
    }

    private boolean isValidRatingEngine(RatingEngine engine) {
        String engineId = engine.getId();
        String engineName = engine.getDisplayName();
        boolean valid = true;
        if (Boolean.TRUE.equals(engine.getDeleted())) {
            log.info("Skip rating engine " + engineId + " : " + engineName + " because it is deleted.");
            valid = false;
        } else if (RatingEngineStatus.INACTIVE.equals(engine.getStatus())) {
            log.info("Skip rating engine " + engineId + " : " + engineName + " because it is inactive.");
            valid = false;
        } else if (engine.getSegment() == null) {
            log.info("Skip rating engine " + engineId + " : " + engineName
                    + " because it belongs to an invalid segment.");
            valid = false;
        } else if (engine.getScoringIteration() == null) {
            log.info("Skip rating engine " + engineId + " : " + engineName
                    + " because it does not have a scoring iteration set.");
            valid = false;
        }
        return valid;
    }

    private boolean isValidCrossSellModel(AIModel model) {
        boolean valid = true;
        if (StringUtils.isBlank(model.getModelSummaryId())) {
            log.warn("AI model " + model.getId() + " does not have an associated model summary.");
            valid = false;
        }
        return valid;
    }

    private boolean isValidCustomEventModel(AIModel model) {
        boolean valid;
        if (StringUtils.isBlank(model.getModelSummaryId())) {
            log.warn("AI model " + model.getId() + " does not have an associated model summary.");
            valid = false;
        } else {
            CustomEventModelingConfig advancedConf = CustomEventModelingConfig.getAdvancedModelingConfig(model);
            valid = CustomEventModelingType.CDL.equals(advancedConf.getCustomEventModelingType());
        }
        return valid;
    }

    private boolean isValidRuleBasedModel(RuleBasedModel model) {
        if (model.getRatingRule() == null) {
            log.warn("Rule based model " + model.getId() + " has null rating rule.");
            return false;
        }
        return true;
    }

    private void initializeRatingLifts() {
        Map<String, Map<String, Double>> liftMap = new HashMap<>();
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        StatisticsContainer statsContainer = dataCollectionProxy.getStats(customerSpace, inactive);
        if (statsContainer != null && statsContainer.getStatsCubes() != null) {
            StatsCube statsCube = statsContainer.getStatsCubes().get(BusinessEntity.Rating.name());
            if (statsCube != null) {
                statsCube.getStatistics().forEach((attrName, attrStats) -> {
                    if (attrName.startsWith(RatingEngine.RATING_ENGINE_PREFIX) && (attrStats.getBuckets() != null)
                            && CollectionUtils.isNotEmpty(attrStats.getBuckets().getBucketList()) //
                            && BucketType.Enum.equals(attrStats.getBuckets().getType())) {
                        Map<String, Double> lifts = new HashMap<>();
                        attrStats.getBuckets().getBucketList().forEach(bucket -> {
                            if (StringUtils.isNotBlank(bucket.getLabel()) && bucket.getLift() != null) {
                                lifts.put(bucket.getLabel(), bucket.getLift());
                            }
                        });
                        if (MapUtils.isNotEmpty(lifts)) {
                            liftMap.put(attrName, lifts);
                        }
                    }
                });
            } else {
                log.info("No stats for Rating in " + inactive);
            }
        } else {
            log.info("No stats at version " + inactive);
        }
        if (MapUtils.isNotEmpty(liftMap)) {
            putObjectInContext(RATING_LIFTS, liftMap);
        }
    }

}
