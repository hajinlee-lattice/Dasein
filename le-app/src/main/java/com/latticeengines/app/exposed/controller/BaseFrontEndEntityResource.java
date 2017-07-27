package com.latticeengines.app.exposed.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.QueryDecorator;
import com.latticeengines.domain.exposed.util.QueryTranslator;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public abstract class BaseFrontEndEntityResource {

    @Autowired
    protected EntityProxy entityProxy;

    @Autowired
    private SegmentProxy segmentProxy;

    protected abstract QueryDecorator getQueryDecorator(boolean addSelects);

    public long getCount(BusinessEntity businessEntity, FrontEndQuery frontEndQuery, String segment) {
        Restriction segmentRestriction = null;
        if (StringUtils.isNotBlank(segment)) {
            segmentRestriction = getSegmentRestriction(segment);
        }
        Query query = QueryTranslator.translate(frontEndQuery, getQueryDecorator(false), segmentRestriction);
        query.addLookup(new EntityLookup(businessEntity));
        return entityProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), query);
    }

    public long getCountForRestriction(BusinessEntity businessEntity, FrontEndRestriction restriction) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        if (restriction != null) {
            frontEndQuery.setFrontEndRestriction(restriction);
        }
        Query query = QueryTranslator.translate(frontEndQuery, getQueryDecorator(false));
        query.addLookup(new EntityLookup(businessEntity));
        return entityProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), query);
    }

    public DataPage getData(FrontEndQuery frontEndQuery, String segment) {
        Restriction segmentRestriction = null;
        if (StringUtils.isNotBlank(segment)) {
            segmentRestriction = getSegmentRestriction(segment);
        }
        Query query = QueryTranslator.translate(frontEndQuery, getQueryDecorator(true), segmentRestriction);
        return entityProxy.getData(MultiTenantContext.getCustomerSpace().toString(), query);
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

}
