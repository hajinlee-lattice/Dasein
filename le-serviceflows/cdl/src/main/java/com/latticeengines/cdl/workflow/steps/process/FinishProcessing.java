package com.latticeengines.cdl.workflow.steps.process;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishProcessing")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(FinishProcessing.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        deleteOrphanTables();

        log.info("Switch data collection to version " + inactive);
        dataCollectionProxy.switchVersion(customerSpace.toString(), inactive);
        log.info("Evict attr repo cache for inactive version " + inactive);
        dataCollectionProxy.evictAttrRepoCache(customerSpace.toString(), inactive);

        // save data collection status history
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        dataCollectionProxy.saveDataCollectionStatusHistory(customerSpace.toString(), detail);

        // TODO: @kliu dataCloudBuildNumber has already been saved to DB in
        // GenerateProcessingReport step.
        if (StringUtils.isNotBlank(configuration.getDataCloudBuildNumber())) {
            dataCollectionProxy.updateDataCloudBuildNumber(customerSpace.toString(),
                    configuration.getDataCloudBuildNumber());
        }
        try {
            // wait for local cache clean up
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignore
        }

        // update bucket metadata and bucketed score summary
        updateBucketMetadata();

        // update segment and rating engine counts
        SegmentCountUtils.invokeMetadataApi(servingStoreProxy, customerSpace.toString());
        SegmentCountUtils.updateEntityCounts(segmentProxy, customerSpace.toString());
        updateActiveRuleModelCounts();
        setPublishedModels();
    }

    private void updateActiveRuleModelCounts() {
        List<RatingModelContainer> containers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        if (CollectionUtils.isNotEmpty(containers)) {
            containers.forEach(container -> {
                if (RatingEngineType.RULE_BASED.equals(container.getEngineSummary().getType())) {
                    String engineId = container.getEngineSummary().getId();
                    try {
                        Map<String, Long> counts = ratingEngineProxy.updateRatingEngineCounts(customerSpace.toString(),
                                engineId);
                        log.info("Updated the counts of rating engine " + engineId + " to "
                                + (MapUtils.isNotEmpty(counts) ? JsonUtils.pprint(counts) : null));
                    } catch (Exception e) {
                        log.error("Failed to update the counts of rating engine " + engineId, e);
                    }
                }
            });
        }
    }

    private void setPublishedModels() {
        List<RatingModelContainer> containers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        if (CollectionUtils.isNotEmpty(containers)) {
            containers.forEach(container -> {
                try {
                    RatingEngine ratingEngine = new RatingEngine();
                    ratingEngine.setId(container.getEngineSummary().getId());
                    ratingEngine.setPublishedIteration(container.getModel());
                    ratingEngineProxy.createOrUpdateRatingEngine(customerSpace.toString(), ratingEngine);
                    log.info("Updated the published iteration  of Rating Engine: " + ratingEngine.getId()
                            + " to Rating model: " + container.getModel().getId());
                } catch (Exception e) {
                    log.error("Failed to update the published Iteration of rating engine: "
                            + container.getEngineSummary().getId() + " and rating model: "
                            + container.getModel().getId(), e);
                }
            });
        }
    }

    private void deleteOrphanTables() {
        List<String> tempTables = getListObjectFromContext(TEMPORARY_CDL_TABLES, String.class);
        if (CollectionUtils.isNotEmpty(tempTables)) {
            List<String> tablesInCollection = dataCollectionProxy.getTableNames(customerSpace.toString(), inactive);
            if (tablesInCollection != null) {
                tempTables.removeAll(tablesInCollection);
            }
            tempTables.forEach(table -> {
                if (StringUtils.isNotBlank(table)) {
                    log.info("Removing orphan table " + table);
                    metadataProxy.deleteTable(customerSpace.toString(), table);
                }
            });
        }
    }

    private void updateBucketMetadata() {
        if (!getConfiguration().isTargetScoreDerivationEnabled()) {
            Map<String, BucketedScoreSummary> bucketedScoreSummaryMap = getMapObjectFromContext(//
                    BUCKETED_SCORE_SUMMARIES_AGG, String.class, BucketedScoreSummary.class);
            if (MapUtils.isNotEmpty(bucketedScoreSummaryMap)) {
                log.info("Found " + bucketedScoreSummaryMap.size() + " bucketed score summaries to update");
                bucketedScoreSummaryMap.forEach((modelGuid, bucketedScoreSummary) -> {
                    log.info("Save bucketed score summary for modelGUID=" + modelGuid + " : "
                            + JsonUtils.serialize(bucketedScoreSummary));
                    bucketedScoreProxy.createOrUpdateBucketedScoreSummary(customerSpace.toString(), modelGuid,
                            bucketedScoreSummary);
                });
            }
        }

        @SuppressWarnings("rawtypes")
        Map<String, List> listMap = getMapObjectFromContext(BUCKET_METADATA_MAP_AGG, String.class, List.class);
        Map<String, String> modelGuidToEngineIdMap = getMapObjectFromContext(MODEL_GUID_ENGINE_ID_MAP_AGG, String.class,
                String.class);
        if (MapUtils.isNotEmpty(listMap)) {
            log.info("Found " + listMap.size() + " bucket metadata lists to update");
            listMap.forEach((modelGuid, list) -> {
                List<BucketMetadata> bucketMetadata = JsonUtils.convertList(list, BucketMetadata.class);
                String engineId = MapUtils.isNotEmpty(modelGuidToEngineIdMap) ? modelGuidToEngineIdMap.get(modelGuid)
                        : null;
                processMetadata(modelGuid, bucketMetadata, engineId);
            });
        }
    }

    private void processMetadata(String modelGuid, List<BucketMetadata> bucketMetadata, String engineId) {
        if (getConfiguration().isTargetScoreDerivationEnabled()) {
            processMetadataWithTargetScoreDerivationEnabled(modelGuid, bucketMetadata, engineId);
        } else {
            processMetadataWithoutTargetScoreDerivationEnabled(modelGuid, bucketMetadata, engineId);
        }
    }

    private void processMetadataWithTargetScoreDerivationEnabled(String modelGuid, List<BucketMetadata> bucketMetadata,
            String engineId) {
        if (bucketMetadata.get(0).getOrigCreationTimestamp() != null && bucketMetadata.get(0).getCreationTimestamp() == bucketMetadata.get(0).getOrigCreationTimestamp()) {
            createMetadataForPublish(modelGuid, bucketMetadata, engineId);
        } else {
            log.info("Updating bucket metadata for modelGUID=" + modelGuid + " : "
                    + JsonUtils.serialize(bucketMetadata));
            if (bucketMetadata.get(0).getOrigCreationTimestamp() != null) {
                updateMetadata(modelGuid, bucketMetadata, true);
            } else {
                updateMetadata(modelGuid, bucketMetadata, false);
                createMetadataForPublish(modelGuid, bucketMetadata, engineId);
            }
        }
    }

    private void updateMetadata(String modelGuid, List<BucketMetadata> bucketMetadata, boolean isPublish) {
        UpdateBucketMetadataRequest request = new UpdateBucketMetadataRequest();
        request.setModelGuid(modelGuid);
        request.setBucketMetadataList(bucketMetadata);
        request.setPublished(isPublish);
        if (!isPublish) {
            bucketMetadata.forEach(b -> b.setOrigCreationTimestamp(b.getCreationTimestamp()));
        }
        bucketedScoreProxy.updateABCDBuckets(customerSpace.toString(), request);
    }

    private void createMetadataForPublish(String modelGuid, List<BucketMetadata> bucketMetadata, String engineId) {
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setModelGuid(modelGuid);
        request.setRatingEngineId(engineId);
        request.setLastModifiedBy(configuration.getUserId());
        request.setBucketMetadataList(bucketMetadata);
        request.setPublished(true);
        request.setCreateForModel(false);
        bucketMetadata.forEach(b -> b.setOrigCreationTimestamp(b.getCreationTimestamp()));
        bucketedScoreProxy.createABCDBuckets(customerSpace.toString(), request);
    }

    private void processMetadataWithoutTargetScoreDerivationEnabled(String modelGuid,
            List<BucketMetadata> bucketMetadata, String engineId) {
        if (bucketMetadata.get(0).getCreationTimestamp() == 0) {
            // actually create bucket metadata
            log.info("Create timestamp is 0, change to create bucketed metadata");
            CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
            request.setModelGuid(modelGuid);
            request.setRatingEngineId(engineId);
            request.setLastModifiedBy(configuration.getUserId());
            request.setBucketMetadataList(bucketMetadata);
            request.setPublished(true);
            bucketedScoreProxy.createABCDBuckets(customerSpace.toString(), request);
        } else {
            log.info("Updating bucket metadata for modelGUID=" + modelGuid + " : "
                    + JsonUtils.serialize(bucketMetadata));
            updateMetadata(modelGuid, bucketMetadata, true);
        }
    }

}
