package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionManagerService;
import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("dataCollectionManagerService")
public class DataCollectionManagerServiceImpl implements DataCollectionManagerService {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionManagerServiceImpl.class);

    private final ProxyResourceService proxyResourceService;

    private final WorkflowProxy workflowProxy;

    private final RatingEngineService ratingEngineService;

    private final EntityProxy entityProxy;

    private final SegmentService segmentService;

    @Resource(name = "localCacheService")
    private CacheService localCacheService;

    @Inject
    public DataCollectionManagerServiceImpl(WorkflowProxy workflowProxy, RatingEngineService ratingEngineService, EntityProxy entityProxy,
                                            SegmentService segmentService, ProxyResourceService proxyResourceService) {
        this.workflowProxy = workflowProxy;
        this.ratingEngineService = ratingEngineService;
        this.entityProxy = entityProxy;
        this.segmentService = segmentService;
        this.proxyResourceService = proxyResourceService;
    }

    @Override
    public boolean resetAll(String customerSpaceStr) {
        DataFeed df = proxyResourceService.getDataFeed(customerSpaceStr);
        DataFeed.Status status = df.getStatus();
        if ((status == DataFeed.Status.Deleting) || (status == DataFeed.Status.Initing)) {
            return true;
        }

        if (status == DataFeed.Status.ProcessAnalyzing) {
            quiesceDataFeed(customerSpaceStr, df);
        }

        proxyResourceService.updateDataFeedStatus(customerSpaceStr, DataFeed.Status.Initing.getName());

        resetBatchStore(customerSpaceStr, BusinessEntity.Contact);
        resetBatchStore(customerSpaceStr, BusinessEntity.Account);

        resetImport(customerSpaceStr);

        return true;

    }

    @Override
    public boolean resetEntity(String customerSpaceStr, BusinessEntity entity) {
        DataFeed df = proxyResourceService.getDataFeed(customerSpaceStr);
        DataFeed.Status status = df.getStatus();
        if ((status == DataFeed.Status.Deleting) || (status == DataFeed.Status.Initing)
                || (status == DataFeed.Status.InitialLoaded)) {
            return true;
        } else if (status == DataFeed.Status.ProcessAnalyzing) {
            return false;
        }
        resetBatchStore(customerSpaceStr, entity);
        proxyResourceService.updateDataFeedStatus(customerSpaceStr, DataFeed.Status.InitialLoaded.getName());
        return true;
    }

    private void stopWorkflow(String customerSpace, Long workflowId) {
        if (workflowId == null) {
            return;
        }
        try {
            Job job = workflowProxy.getWorkflowExecution(workflowId.toString(), customerSpace);
            if ((job != null) && (job.isRunning())) {
                workflowProxy.stopWorkflowExecution(workflowId.toString(), customerSpace);
            }
        } catch (Exception e) {
            log.error("Failed to stop workflow " + workflowId, e);
        }
    }

    private void quiesceDataFeed(String customerSpaceStr, DataFeed df) {
        DataFeedExecution exec = df.getActiveExecution();
        if (exec != null) {
            stopWorkflow(customerSpaceStr, exec.getWorkflowId());
            proxyResourceService.finishExecution(customerSpaceStr, DataFeed.Status.Active.getName());
        }
    }

    private void resetImport(String customerSpaceStr) {
        proxyResourceService.resetImport(customerSpaceStr);
    }

    private void resetBatchStore(String customerSpaceStr, BusinessEntity entity) {
        proxyResourceService.resetTable(customerSpaceStr, entity.getBatchStore());
    }

    @Override
    public void refreshCounts(String customerSpace) {
        List<MetadataSegment> segments = segmentService.getSegments();
        if (CollectionUtils.isNotEmpty(segments)) {
            segments.forEach(segment -> {
                MetadataSegment segmentCopy = JsonUtils.deserialize(JsonUtils.serialize(segment),
                        MetadataSegment.class);
                for (BusinessEntity entity : BusinessEntity.COUNT_ENTITIES) {
                    try {
                        Long count = getEntityCount(customerSpace, entity, segmentCopy);
                        segment.setEntityCount(entity, count);
                        log.info("Set " + entity + " count of segment " + segment.getName() + " to " + count);
                    } catch (Exception e) {
                        log.error("Failed to get " + entity + " count for segment " + segment.getName());
                    }
                    segmentService.createOrUpdateSegment(segment);
                    proxyResourceService.registerAction(ActionContext.getAction(), "DEFAULT_USER");
                }
                updateRatingEngineCounts(segment.getName());
            });
        }
    }

    private Long getEntityCount(String customerSpace, BusinessEntity entity, MetadataSegment segment) {
        if (segment == null) {
            return null;
        }
        FrontEndQuery frontEndQuery = segment.toFrontEndQuery(entity);
        return entityProxy.getCount(customerSpace, frontEndQuery);
    }

    private void updateRatingEngineCounts(String segmentName) {
        List<String> ratingEngineIds = ratingEngineService.getAllRatingEngineIdsInSegment(segmentName);
        if (CollectionUtils.isNotEmpty(ratingEngineIds)) {
            ratingEngineIds.forEach(engineId -> {
                try {
                    Map<String, Long> counts = ratingEngineService.updateRatingEngineCounts(engineId);
                    log.info("Updated the counts of rating engine " + engineId + " to "
                            + (MapUtils.isNotEmpty(counts) ? JsonUtils.pprint(counts) : null));
                } catch (Exception e) {
                    log.error("Failed to update the counts of rating engine " + engineId, e);
                }
            });
        }
    }
}
