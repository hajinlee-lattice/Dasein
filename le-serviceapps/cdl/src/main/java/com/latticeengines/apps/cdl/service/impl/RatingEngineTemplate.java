package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public abstract class RatingEngineTemplate {

    @Value("${common.pls.url}")
    protected String internalResourceHostPort;

    protected InternalResourceRestApiProxy internalResourceProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private AIModelEntityMgr aiModelEntityMgr;

    @VisibleForTesting
    RatingEngineSummary constructRatingEngineSummary(RatingEngine ratingEngine, String tenantId) {
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

        MetadataSegment segment = ratingEngine.getSegment();
        if (segment != null) {
            ratingEngineSummary.setAccountsInSegment(segment.getAccounts());
            ratingEngineSummary.setContactsInSegment(segment.getContacts());
        }

        if (ratingEngine.getType() != RatingEngineType.RULE_BASED) {
            AIModel aimodel;
            if (ratingEngine.getActiveModel() == null) {
                aimodel = aiModelEntityMgr.findByField("pid", ratingEngine.getActiveModelPid());
            } else {
                aimodel = (AIModel) ratingEngine.getActiveModel();
            }
            if (aimodel.getModelSummary() != null) {
                ratingEngineSummary.setBucketMetadata(internalResourceProxy
                        .getUpToDateABCDBuckets(aimodel.getModelSummary().getId(), CustomerSpace.parse(tenantId)));
            }
        } else {
            Map<String, Long> counts = ratingEngine.getCountsAsMap();
            if (counts != null)
                ratingEngineSummary.setBucketMetadata(counts.keySet().stream()
                        .map(c -> new BucketMetadata(BucketName.fromValue(c), counts.get(c).intValue()))
                        .collect(Collectors.toList()));
        }

        Date lastRefreshedDate = findLastRefreshedDate(tenantId);
        ratingEngineSummary.setLastRefreshedDate(lastRefreshedDate);
        return ratingEngineSummary;
    }

    Date findLastRefreshedDate(String tenantId) {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(tenantId);
        return dataFeed.getLastPublished();
    }

    protected void initializeInternalResourceRestApiProxy() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }
}
