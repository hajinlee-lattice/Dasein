package com.latticeengines.app.exposed.controller;

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
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public abstract class BaseFrontEndEntityResource {

    private final EntityProxy entityProxy;

    private final SegmentProxy segmentProxy;

    private final LoadingCache<String, Long> countCache;

    private final LoadingCache<String, DataPage> dataCache;

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
    }

    public long getCount(FrontEndQuery frontEndQuery, String segment) {
        appendSegmentRestriction(frontEndQuery, segment);
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return countCache.get(String.format("%s:%s", tenantId, JsonUtils.serialize(frontEndQuery)));
    }

    private long getCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf(":"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        return entityProxy.getCount(tenantId, getMainEntity(), frontEndQuery);
    }

    public long getCountForRestriction(FrontEndRestriction restriction) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        if (restriction != null) {
            frontEndQuery.setFrontEndRestriction(restriction);
        }
        return entityProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), getMainEntity(), frontEndQuery);
    }

    public DataPage getData(FrontEndQuery frontEndQuery, String segment) {
        appendSegmentRestriction(frontEndQuery, segment);
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return dataCache.get(String.format("%s:%s", tenantId, JsonUtils.serialize(frontEndQuery)));
    }

    private DataPage getDataFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf(":"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        return entityProxy.getData(tenantId, getMainEntity(), frontEndQuery);
    }

    private void appendSegmentRestriction(FrontEndQuery frontEndQuery, String segment) {
        Restriction segmentRestriction = null;
        if (StringUtils.isNotBlank(segment)) {
            segmentRestriction = getSegmentRestriction(segment);
        }
        Restriction frontEndRestriction = null;
        if (frontEndQuery.getFrontEndRestriction() != null) {
            frontEndRestriction = frontEndQuery.getFrontEndRestriction().getRestriction();
        }
        if (segmentRestriction != null && frontEndRestriction != null) {
            Restriction totalRestriction = Restriction.builder().and(frontEndRestriction, segmentRestriction).build();
            frontEndQuery.getFrontEndRestriction().setRestriction(totalRestriction);
        } else if (segmentRestriction != null) {
            frontEndQuery.getFrontEndRestriction().setRestriction(segmentRestriction);
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

    abstract BusinessEntity getMainEntity();

}
