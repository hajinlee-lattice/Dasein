package com.latticeengines.pls.controller.datacollection;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
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

    public long getCount(FrontEndQuery frontEndQuery) {
        appendSegmentRestriction(frontEndQuery);
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setMainEntity(getMainEntity());
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return entityProxy.getCount(tenantId, frontEndQuery);
    }

    public DataPage getData(FrontEndQuery frontEndQuery) {
        appendSegmentRestriction(frontEndQuery);
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setMainEntity(getMainEntity());
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return entityProxy.getData(tenantId, frontEndQuery);
    }

    public Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery) {
        appendSegmentRestriction(frontEndQuery);
        optimizeRestrictions(frontEndQuery);
        frontEndQuery.setMainEntity(getMainEntity());
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return entityProxy.getRatingCount(tenantId, frontEndQuery);
    }

    private void appendSegmentRestriction(FrontEndQuery frontEndQuery) {
        Restriction segmentRestriction = null;
        if (StringUtils.isNotBlank(frontEndQuery.getPreexistingSegmentName())) {
            segmentRestriction = getSegmentRestriction(frontEndQuery.getPreexistingSegmentName());
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

    // TODO Bernard JiunJiun - handle contactRestriction
    private Restriction getSegmentRestriction(String segmentName) {
        MetadataSegment segment = segmentProxy
                .getMetadataSegmentByName(MultiTenantContext.getCustomerSpace().toString(), segmentName);
        if (segment != null) {
            return segment.getAccountRestriction();
        } else {
            return null;
        }
    }

    private void optimizeRestrictions(FrontEndQuery frontEndQuery) {
        if (frontEndQuery.getAccountRestriction() != null) {
            Restriction restriction = frontEndQuery.getAccountRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getAccountRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
        if (frontEndQuery.getContactRestriction() != null) {
            Restriction restriction = frontEndQuery.getContactRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getContactRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
    }

    abstract BusinessEntity getMainEntity();

}
