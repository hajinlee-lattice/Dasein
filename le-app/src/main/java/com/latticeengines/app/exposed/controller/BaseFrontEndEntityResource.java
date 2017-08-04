package com.latticeengines.app.exposed.controller;

import org.apache.commons.lang3.StringUtils;

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

    BaseFrontEndEntityResource(EntityProxy entityProxy, SegmentProxy segmentProxy) {
        this.entityProxy = entityProxy;
        this.segmentProxy = segmentProxy;
    }

    public long getCount(FrontEndQuery frontEndQuery, String segment) {
        appendSegmentRestriction(frontEndQuery, segment);
        return entityProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), getMainEntity(), frontEndQuery);
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
        return entityProxy.getData(MultiTenantContext.getCustomerSpace().toString(), getMainEntity(), frontEndQuery);
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
