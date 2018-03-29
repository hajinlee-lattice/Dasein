package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public abstract class RatingEngineTemplate {

    @Value("${common.pls.url}")
    protected String internalResourceHostPort;

    protected InternalResourceRestApiProxy internalResourceProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

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

        if (ratingEngine.getActiveModel() != null && ratingEngine.getActiveModel() instanceof AIModel) {
            ModelSummary modelSummary = ((AIModel) ratingEngine.getActiveModel()).getModelSummary();
            if (modelSummary != null) {
                ratingEngineSummary.setBucketMetadata(internalResourceProxy
                        .getUpToDateABCDBucketsByRatingEngineId(ratingEngine.getId(), CustomerSpace.parse(tenantId)));
            }
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
