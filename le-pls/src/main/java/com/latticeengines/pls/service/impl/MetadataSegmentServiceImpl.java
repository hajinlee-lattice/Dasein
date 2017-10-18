package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.latticeengines.security.exposed.util.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Service("metadataSegmentService")
public class MetadataSegmentServiceImpl implements MetadataSegmentService {
    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentServiceImpl.class);

    private final SegmentProxy segmentProxy;

    @Inject
    public MetadataSegmentServiceImpl(SegmentProxy segmentProxy) {
        this.segmentProxy = segmentProxy;
    }

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
        return getSegmentByName(name, true);
    }

    @Override
    public MetadataSegment getSegmentByName(String name, boolean shouldTransateForFrontend) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace, name);
        if (shouldTransateForFrontend) {
            segment = translateForFrontend(segment);
        }
        return segment;
    }

    @Override
    public MetadataSegmentDTO getSegmentDTOByName(String name, boolean shouldTransateForFrontend) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        MetadataSegmentDTO segmentDTO = segmentProxy.getMetadataSegmentWithPidByName(customerSpace, name);
        if (shouldTransateForFrontend) {
            segmentDTO.setMetadataSegment(translateForFrontend(segmentDTO.getMetadataSegment()));
        }
        return segmentDTO;
    }

    @Override
    public MetadataSegment createOrUpdateSegment(MetadataSegment segment) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        translateForBackend(segment);
        return translateForFrontend(segmentProxy.createOrUpdateSegment(customerSpace, segment));
    }

    @Override
    public void deleteSegmentByName(String name) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        segmentProxy.deleteSegmentByName(customerSpace, name);
    }

    private MetadataSegment translateForBackend(MetadataSegment segment) {
        try {
            FrontEndRestriction accountFrontEndRestriction = segment.getAccountFrontEndRestriction();
            segment.setAccountRestriction(accountFrontEndRestriction.getRestriction());
            segment.setAccountFrontEndRestriction(null);
            FrontEndRestriction contactFrontEndRestriction = segment.getContactFrontEndRestriction();
            segment.setContactRestriction(contactFrontEndRestriction.getRestriction());
            segment.setContactFrontEndRestriction(null);
        } catch (Exception e) {
            log.error("Encountered error translating frontend restriction for segment with name " + segment.getName(),
                    e);
        }
        return segment;
    }

    private MetadataSegment translateForFrontend(MetadataSegment segment) {
        if (segment == null) {
            return null;
        }
        try {
            Restriction accountRestriction = segment.getAccountRestriction();
            if (accountRestriction != null) {
                FrontEndRestriction accountFrontEndRestriction = new FrontEndRestriction();
                accountFrontEndRestriction.setRestriction(accountRestriction);
                segment.setAccountFrontEndRestriction(accountFrontEndRestriction);
                segment.setAccountRestriction(null);
            }
            Restriction contactRestriction = segment.getContactRestriction();
            if (contactRestriction != null) {
                FrontEndRestriction contactFrontEndRestriction = new FrontEndRestriction();
                contactFrontEndRestriction.setRestriction(contactRestriction);
                segment.setContactFrontEndRestriction(contactFrontEndRestriction);
                segment.setContactRestriction(null);
            }

            if (Boolean.FALSE.equals(segment.getMasterSegment())) {
                segment.setMasterSegment(null);
            }
        } catch (Exception e) {
            log.error("Encountered error translating backend restriction for segment with name  " + segment.getName(),
                    e);
        }
        return segment;
    }

}
