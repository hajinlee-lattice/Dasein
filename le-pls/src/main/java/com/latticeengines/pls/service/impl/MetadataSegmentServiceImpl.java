package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.QueryTranslator;
import com.latticeengines.domain.exposed.util.ReverseQueryTranslator;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("metadataSegmentService")
public class MetadataSegmentServiceImpl implements MetadataSegmentService {
    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentServiceImpl.class);

    @Autowired
    private SegmentProxy segmentProxy;

    @Override
    public List<MetadataSegment> getSegments() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        List<MetadataSegment> backendSegments = segmentProxy.getMetadataSegments(customerSpace);
        if (backendSegments == null) {
            return null;
        } else {
            return segmentProxy.getMetadataSegments(customerSpace).stream() //
                    .map(this::translateForFrontend).collect(Collectors.toList());
        }
    }

    @Override
    public MetadataSegment getSegmentByName(String name) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return translateForFrontend(segmentProxy.getMetadataSegmentByName(customerSpace, name));
    }

    @Override
    public MetadataSegment createOrUpdateSegment(MetadataSegment segment) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        translateForBackend(segment);
        updateStatistics(segment);
        return translateForFrontend(segmentProxy.createOrUpdateSegment(customerSpace, segment));
    }

    @Override
    public void deleteSegmentByName(String name) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        segmentProxy.deleteSegmentByName(customerSpace, name);
    }

    private MetadataSegment translateForBackend(MetadataSegment segment) {
        try {
            FrontEndRestriction frontEndRestriction = segment.getSimpleRestriction();
            segment.setRestriction(QueryTranslator.translateFrontEndRestriction(frontEndRestriction));
        } catch (Exception e) {
            log.error("Encountered error translating frontend restriction for segment with name %s", e);
        }
        return segment;
    }

    private MetadataSegment translateForFrontend(MetadataSegment segment) {
        try {
            Restriction restriction = segment.getRestriction();
            segment.setSimpleRestriction(ReverseQueryTranslator.translateRestriction(restriction));
            segment.setRestriction(null);
        } catch (Exception e) {
            log.error("Encountered error translating backend restriction for segment with name %s", e);
        }
        return segment;
    }

    private void updateStatistics(MetadataSegment segment) {
        // TODO: put random count there, before we have data in redshift.
        Random random = new Random(System.currentTimeMillis());
        segment.getSegmentPropertyBag().set(MetadataSegmentPropertyName.NumAccounts, random.nextInt(1000));

//        Query query = Query.builder().where(segment.getRestriction()).build();
//        try {
//            long count = accountProxy.getCount(MultiTenantContext.getTenant().getId(), query);
//            segment.getSegmentPropertyBag().set(MetadataSegmentPropertyName.NumAccounts, count);
//        } catch (Exception e) {
//            log.error(String.format("Failed to update statistics for segment %s", segment.getName()), e);
//        }
    }
}
