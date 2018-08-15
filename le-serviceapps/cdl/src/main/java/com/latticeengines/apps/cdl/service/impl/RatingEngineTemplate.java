package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;

public abstract class RatingEngineTemplate {

    private static Logger log = LoggerFactory.getLogger(RatingEngineTemplate.class);

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @VisibleForTesting
    RatingEngineSummary constructRatingEngineSummary(RatingEngine ratingEngine, String tenantId) {
        Date lastRefreshedDate = findLastRefreshedDate(tenantId);
        return constructRatingEngineSummary(ratingEngine, tenantId, lastRefreshedDate);
    }

    RatingEngineSummary constructRatingEngineSummary(RatingEngine ratingEngine, String tenantId,
            Date lastRefreshedDate) {
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

        MetadataSegment segment = ratingEngine.getSegment();
        if (segment != null) {
            ratingEngineSummary.setAccountsInSegment(segment.getAccounts());
            ratingEngineSummary.setContactsInSegment(segment.getContacts());
        }

        try {
            if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
                Map<String, Long> counts = ratingEngine.getCountsAsMap();
                if (counts != null)
                    ratingEngineSummary.setBucketMetadata(counts.keySet().stream()
                            .map(c -> new BucketMetadata(BucketName.fromValue(c), counts.get(c).intValue()))
                            .collect(Collectors.toList()));
            } else {
                if (ratingEngine.getPublishedIteration() != null && ((AIModel) ratingEngine.getPublishedIteration())
                        .getModelingJobStatus() == JobStatus.COMPLETED) {
                    List<BucketMetadata> bucketMetadataList = bucketedScoreProxy.getLatestABCDBucketsByModelGuid(
                            tenantId, ((AIModel) ratingEngine.getPublishedIteration()).getModelSummaryId());
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

    Date findLastRefreshedDate(String tenantId) {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(tenantId);
        return dataFeed.getLastPublished();
    }
}
