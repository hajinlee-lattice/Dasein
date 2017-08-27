package com.latticeengines.app.exposed.controller;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public abstract class BaseFrontEndEntityResource {

    private final EntityProxy entityProxy;

    private final SegmentProxy segmentProxy;

    private final LoadingCache<String, Long> countCache;

    private final LoadingCache<String, DataPage> dataCache;

    private final LoadingCache<String, Map<String, Long>> ratingCache;

    BaseFrontEndEntityResource(EntityProxy entityProxy, SegmentProxy segmentProxy) {
        this.entityProxy = entityProxy;
        this.segmentProxy = segmentProxy;
        countCache = Caffeine.newBuilder() //
                .maximumSize(1000) //
                .expireAfterWrite(10, TimeUnit.MINUTES) //
                .refreshAfterWrite(1, TimeUnit.MINUTES) //
                .build(this::getCountFromObjectApi);
        dataCache = Caffeine.newBuilder() //
                .maximumSize(100) //
                .expireAfterWrite(10, TimeUnit.MINUTES) //
                .refreshAfterWrite(1, TimeUnit.MINUTES) //
                .build(this::getDataFromObjectApi);
        ratingCache = Caffeine.newBuilder() //
                .maximumSize(1000) //
                .expireAfterWrite(10, TimeUnit.MINUTES) //
                .refreshAfterWrite(1, TimeUnit.MINUTES) //
                .build(this::getRatingCountFromObjectApi);
    }

    public long getCount(FrontEndQuery frontEndQuery, String segment) {
        appendSegmentRestriction(frontEndQuery, segment);
        optimizeRestriction(frontEndQuery);
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return countCache.get(String.format("%s:%s", tenantId, JsonUtils.serialize(frontEndQuery)));
    }

    private long getCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf(":"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        frontEndQuery.setMainEntity(getMainEntity());
        return entityProxy.getCount(tenantId, frontEndQuery);
    }

    public long getCountForRestriction(FrontEndRestriction restriction) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        if (restriction != null) {
            frontEndQuery.setAccountRestriction(restriction);
        }
        optimizeRestriction(frontEndQuery);
        return getCount(frontEndQuery, null);
    }

    public DataPage getData(FrontEndQuery frontEndQuery, String segment) {
        appendSegmentRestriction(frontEndQuery, segment);
        optimizeRestriction(frontEndQuery);
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return dataCache.get(String.format("%s:%s", tenantId, JsonUtils.serialize(frontEndQuery)));
    }

    public Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery, String segment) {
        appendSegmentRestriction(frontEndQuery, segment);
        optimizeRestriction(frontEndQuery);
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return ratingCache.get(String.format("%s:%s", tenantId, JsonUtils.serialize(frontEndQuery)));
    }

    private DataPage getDataFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf(":"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        frontEndQuery.setMainEntity(getMainEntity());
        return entityProxy.getData(tenantId, frontEndQuery);
    }

    private Map<String, Long> getRatingCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf(":"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        frontEndQuery.setMainEntity(getMainEntity());
        return entityProxy.getRatingCount(tenantId, frontEndQuery);
    }

    private void appendSegmentRestriction(FrontEndQuery frontEndQuery, String segment) {
        Restriction segmentRestriction = null;
        if (StringUtils.isNotBlank(segment)) {
            segmentRestriction = getSegmentRestriction(segment);
        }
        Restriction frontEndRestriction = null;
        if (frontEndQuery.getAccountRestriction() != null) {
            frontEndRestriction = frontEndQuery.getAccountRestriction().getRestriction();
        }
        if (segmentRestriction != null && frontEndRestriction != null) {
            Restriction totalRestriction = Restriction.builder().and(frontEndRestriction, segmentRestriction).build();
            frontEndQuery.getAccountRestriction().setRestriction(totalRestriction);
        } else if (segmentRestriction != null) {
            frontEndQuery.getAccountRestriction().setRestriction(segmentRestriction);
        }
    }

    private Restriction getSegmentRestriction(String segmentName) {
        MetadataSegment segment = segmentProxy
                .getMetadataSegmentByName(MultiTenantContext.getCustomerSpace().toString(), segmentName);
        if (segment != null) {
            return segment.getRestriction();
        } else {
            return null;
        }
    }

    private void optimizeRestriction(FrontEndQuery frontEndQuery) {
        if (frontEndQuery.getAccountRestriction() != null) {
            Restriction restriction = frontEndQuery.getAccountRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getAccountRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
    }

    abstract BusinessEntity getMainEntity();

}
