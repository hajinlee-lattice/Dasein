package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.QueryTranslator;
import com.latticeengines.domain.exposed.util.ReverseQueryTranslator;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.AccountProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class MetadataSegmentServiceImpl implements MetadataSegmentService {
    private static final Log log = LogFactory.getLog(MetadataSegmentServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private AccountProxy accountProxy;

    @Override
    public List<MetadataSegment> getSegments() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return metadataProxy.getMetadataSegments(customerSpace).stream() //
                .map(this::translateForFrontend).collect(Collectors.toList());
    }

    @Override
    public MetadataSegment getSegmentByName(String name) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return translateForFrontend(metadataProxy.getMetadataSegmentByName(customerSpace, name));
    }

    @Override
    public MetadataSegment createOrUpdateSegment(MetadataSegment segment) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        translateForBackend(segment);
        updateStatistics(segment);
        return translateForFrontend(metadataProxy.createOrUpdateSegment(customerSpace, segment));
    }

    @Override
    public void deleteSegmentByName(String name) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        metadataProxy.deleteSegmentByName(customerSpace, name);
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
        } catch (Exception e) {
            log.error("Encountered error translating backend restriction for segment with name %s", e);
        }
        return segment;
    }

    private void updateStatistics(MetadataSegment segment) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> doUpdateStatistics(segment));
    }

    private void doUpdateStatistics(MetadataSegment segment) {
        Query query = new Query();
        query.setRestriction(segment.getRestriction());

        long count = accountProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), query);
        segment.getSegmentPropertyBag().set(MetadataSegmentPropertyName.NumAccounts, count);
    }
}
