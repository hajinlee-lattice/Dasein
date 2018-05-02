package com.latticeengines.cdl.workflow.steps.process;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
        cleanupEntityTableMap(getMapObjectFromContext(ENTITY_DIFF_TABLES, BusinessEntity.class, String.class));
    }

    private void cleanupEntityTableMap(Map<BusinessEntity, String> entityTableNames) {
        if (MapUtils.isNotEmpty(entityTableNames)) {
            entityTableNames.forEach((entity, tableName) -> {
                String servingStoreName = dataCollectionProxy.getTableName(customerSpace.toString(),
                        entity.getServingStore(), inactive);
                if (!tableName.equals(servingStoreName)) {
                    log.info("Removing orphan table " + tableName);
                    metadataProxy.deleteTable(customerSpace.toString(), tableName);
                }
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
    }

}
