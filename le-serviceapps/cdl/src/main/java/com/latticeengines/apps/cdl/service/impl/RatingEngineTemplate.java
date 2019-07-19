package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;

public abstract class RatingEngineTemplate {

    private static Logger log = LoggerFactory.getLogger(RatingEngineTemplate.class);

    private ExecutorService tpForParallelStream;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private DataFeedService dataFeedService;

    List<RatingEngineSummary> constructRatingEngineSummaries(List<RatingEngine> ratingList, String tenantId,
            Date lastRefreshedDate) {
        List<String> modelSummaryIds = ratingList.stream() //
                .filter(re -> re.getPublishedIteration() != null && re.getPublishedIteration() instanceof AIModel) //
                .map(re -> (AIModel) re.getPublishedIteration()) //
                .filter(ai -> StringUtils.isNotBlank(ai.getModelSummaryId())) //
                .map(AIModel::getModelSummaryId) //
                .collect(Collectors.toList());

        Map<String, List<BucketMetadata>> modelSummaryToBucketListMap = bucketedScoreProxy
                .getAllPublishedBucketMetadataByModelSummaryIdList(tenantId, modelSummaryIds);

        List<Callable<RatingEngineSummary>> callables = ratingList.stream().map(re -> //
        (Callable<RatingEngineSummary>) () -> //
        constructRatingEngineSummary(re, tenantId, lastRefreshedDate, modelSummaryToBucketListMap)) //
                .collect(Collectors.toList());

        return ThreadPoolUtils.runCallablesInParallel(getTpForParallelStream(), callables, 30, 1);
    }

    @VisibleForTesting
    RatingEngineSummary constructRatingEngineSummary(RatingEngine ratingEngine, String tenantId) {
        Date lastRefreshedDate = findLastRefreshedDate(tenantId);
        return constructRatingEngineSummary(ratingEngine, tenantId, lastRefreshedDate, null);
    }

    RatingEngineSummary constructRatingEngineSummary(RatingEngine ratingEngine, String tenantId, Date lastRefreshedDate,
            Map<String, List<BucketMetadata>> modelSummaryToBucketListMap) {
        if (ratingEngine == null) {
            return null;
        }
        RatingEngineSummary ratingEngineSummary = new RatingEngineSummary();
        ratingEngineSummary.setId(ratingEngine.getId());
        ratingEngineSummary.setDisplayName(ratingEngine.getDisplayName());
        ratingEngineSummary.setNote(ratingEngine.getNote());
        ratingEngineSummary.setType(ratingEngine.getType());
        ratingEngineSummary.setStatus(ratingEngine.getStatus());
        ratingEngineSummary.setDeleted(ratingEngine.getDeleted());
        ratingEngineSummary.setSegmentDisplayName(
                ratingEngine.getSegment() != null ? ratingEngine.getSegment().getDisplayName() : null);
        ratingEngineSummary
                .setSegmentName(ratingEngine.getSegment() != null ? ratingEngine.getSegment().getName() : null);
        ratingEngineSummary.setCreatedBy(ratingEngine.getCreatedBy());
        ratingEngineSummary.setUpdatedBy(ratingEngine.getUpdatedBy());
        ratingEngineSummary.setCreated(ratingEngine.getCreated());
        ratingEngineSummary.setUpdated(ratingEngine.getUpdated());
        ratingEngineSummary.setCoverage(ratingEngine.getCountsAsMap());
        ratingEngineSummary.setAdvancedRatingConfig(ratingEngine.getAdvancedRatingConfig());
        ratingEngineSummary.setPublished(ratingEngine.getPublishedIteration() != null);
        ratingEngineSummary.setLatestIterationId(ratingEngine.getLatestIteration().getId());
        ratingEngineSummary.setScoringIterationId(
                ratingEngine.getScoringIteration() != null ? ratingEngine.getScoringIteration().getId() : null);
        ratingEngineSummary.setPublishedIterationId(
                ratingEngine.getPublishedIteration() != null ? ratingEngine.getPublishedIteration().getId() : null);

        if (ratingEngine.getType() == RatingEngineType.CROSS_SELL
                || ratingEngine.getType() == RatingEngineType.CUSTOM_EVENT) {
            Boolean completed = ratingEngine.getLatestIteration().getIteration() != 1
                    || ((AIModel) ratingEngine.getLatestIteration()).getModelingJobStatus() != JobStatus.PENDING;
            ratingEngineSummary.setCompleted(completed);
        } else if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
            ratingEngineSummary.setCompleted(true);
        }

        MetadataSegment segment = ratingEngine.getSegment();
        if (segment != null) {
            ratingEngineSummary.setAccountsInSegment(segment.getAccounts());
            ratingEngineSummary.setContactsInSegment(segment.getContacts());
        }

        try {
            if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
                Map<String, Long> counts = ratingEngine.getCountsAsMap();
                if (counts != null) {
                    ratingEngineSummary.setBucketMetadata(counts.keySet().stream()
                            .map(c -> new BucketMetadata(BucketName.fromValue(c), counts.get(c).intValue()))
                            .collect(Collectors.toList()));
                }
            } else {
                if (ratingEngine.getPublishedIteration() != null
                        && StringUtils.isNotEmpty(((AIModel) ratingEngine.getPublishedIteration()).getModelSummaryId())
                        && ((AIModel) ratingEngine.getPublishedIteration())
                                .getModelingJobStatus() == JobStatus.COMPLETED) {

                    List<BucketMetadata> bucketMetadataList = null;
                    String modelSummaryId = ((AIModel) ratingEngine.getPublishedIteration()).getModelSummaryId();
                    if (MapUtils.isNotEmpty(modelSummaryToBucketListMap)) {
                        bucketMetadataList = modelSummaryToBucketListMap.get(modelSummaryId);
                    } else {
                        bucketMetadataList = bucketedScoreProxy.getPublishedBucketMetadataByModelGuid(tenantId,
                                modelSummaryId);
                    }
                    if (CollectionUtils.isEmpty(bucketMetadataList)) {
                        bucketMetadataList = setAndRetrievePublishedBucketsIfMissing(tenantId, ratingEngine);
                    }
                    bucketMetadataList = BucketedScoreSummaryUtils.sortBucketMetadata(bucketMetadataList, false);
                    ratingEngineSummary.setBucketMetadata(bucketMetadataList);
                }
            }
        } catch (Exception ex) {
            log.error("Unable to populate bucket metadata for rating engine :" + ratingEngine.getId(), ex);
        }
        ratingEngineSummary.setLastRefreshedDate(lastRefreshedDate);
        return ratingEngineSummary;
    }

    protected boolean isPublishedMetadataMissing(String tenantId, RatingEngine ratingEngine) {
        return ratingEngine.getType() != RatingEngineType.RULE_BASED && ratingEngine.getPublishedIteration() != null
                && ((AIModel) ratingEngine.getPublishedIteration()).getModelSummaryId() != null
                && ((AIModel) ratingEngine.getPublishedIteration()).getModelingJobStatus() == JobStatus.COMPLETED
                && CollectionUtils.isEmpty(bucketedScoreProxy.getPublishedBucketMetadataByModelGuid(tenantId,
                        ((AIModel) ratingEngine.getPublishedIteration()).getModelSummaryId()));

    }

    protected List<BucketMetadata> setAndRetrievePublishedBucketsIfMissing(String tenantId, RatingEngine ratingEngine) {
        validateAIRatingEngine(ratingEngine);
        if (isPublishedMetadataMissing(tenantId, ratingEngine)) {
            String modelSummaryId = ((AIModel) ratingEngine.getPublishedIteration()).getModelSummaryId();
            List<BucketMetadata> latestBuckets = bucketedScoreProxy.getLatestABCDBucketsByModelGuid(tenantId,
                    modelSummaryId);
            UpdateBucketMetadataRequest request = new UpdateBucketMetadataRequest();
            request.setBucketMetadataList(latestBuckets);
            request.setModelGuid(modelSummaryId);
            request.setPublished(true);
            return bucketedScoreProxy.updateABCDBuckets(tenantId, request);
        }
        return new ArrayList<>();
    }

    protected void validateAIRatingEngine(RatingEngine ratingEngine) {
        if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { RatingEngineType.RULE_BASED.getRatingEngineTypeName() });
        }
    }

    Date findLastRefreshedDate(String tenantId) {
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(CustomerSpace.parse(tenantId).toString());
        return dataFeed.getLastPublished();
    }

    private ExecutorService getTpForParallelStream() {
        if (tpForParallelStream == null) {
            synchronized (this) {
                if (tpForParallelStream == null) {
                    tpForParallelStream = ThreadPoolUtils.getCachedThreadPool("re-template");
                }
            }
        }
        return tpForParallelStream;
    }
}
