package com.latticeengines.cdl.workflow.steps.process;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishProcessing")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

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
        SegmentCountUtils.updateEntityCounts(segmentProxy, entityProxy, customerSpace.toString());
        RatingEngineCountUtils.updateRatingEngineCounts(ratingEngineProxy, customerSpace.toString());
    }

    private void deleteOrphanTables() {
        List<String> tempTables = getListObjectFromContext(TEMPORARY_CDL_TABLES, String.class);
        if (CollectionUtils.isNotEmpty(tempTables)) {
            List<String> tablesInCollection = dataCollectionProxy.getTableNames(customerSpace.toString(), inactive);
            if (tablesInCollection != null) {
                tempTables.removeAll(tablesInCollection);
            }
            tempTables.forEach(table -> {
                log.info("Removing orphan table " + table);
                metadataProxy.deleteTable(customerSpace.toString(), table);
            });
        }
    }

    private void updateBucketMetadata() {
        Map<String, BucketedScoreSummary> bucketedScoreSummaryMap = getMapObjectFromContext(BUCKETED_SCORE_SUMMARIES,
                String.class, BucketedScoreSummary.class);
        if (MapUtils.isNotEmpty(bucketedScoreSummaryMap)) {
            log.info("Found " + bucketedScoreSummaryMap.size() + " bucketed score summaries to update");
            bucketedScoreSummaryMap.forEach((modelGuid, bucketedScoreSummary) -> {
                log.info("Save bucketed score summary for modelGUID=" + modelGuid + " : "
                        + JsonUtils.serialize(bucketedScoreSummary));
                bucketedScoreProxy.createOrUpdateBucketedScoreSummary(customerSpace.toString(), modelGuid, bucketedScoreSummary);
            });
        }
        Map<String, List> listMap = getMapObjectFromContext(BUCKET_METADATA_MAP, String.class, List.class);
        if (MapUtils.isNotEmpty(listMap)) {
            log.info("Found " + listMap.size() + " bucket metadata lists to update");
            listMap.forEach((modelGuid, list) -> {
                List<BucketMetadata> bucketMetadata = JsonUtils.convertList(list, BucketMetadata.class);
                UpdateBucketMetadataRequest request = new UpdateBucketMetadataRequest();
                request.setModelGuid(modelGuid);
                request.setBucketMetadataList(bucketMetadata);
                bucketedScoreProxy.updateABCDBuckets(customerSpace.toString(), request);
            });
        }
    }

}
