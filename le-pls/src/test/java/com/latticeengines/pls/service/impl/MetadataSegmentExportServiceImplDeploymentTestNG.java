package com.latticeengines.pls.service.impl;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.ExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class MetadataSegmentExportServiceImplDeploymentTestNG extends PlsFunctionalTestNGBase {
    private final static String SEGMENT_NAME = "segment";
    private final static String CREATED_BY = "lattice@lattice-engines.com";

    @Autowired
    private MetadataSegmentExportService metadataSegmentExportService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private SegmentService segmentService;

    private MetadataSegment segment;

    private Tenant tenant;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        tenant = testBed.getTestTenants().get(0);
        MultiTenantContext.setTenant(tenant);

        segment = new MetadataSegment();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService.findByName(CustomerSpace.parse(tenant.getId()).toString(),
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);

    }

    @AfterClass(groups = "deployment")
    public void teardown() throws Exception {
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }

    @Test(groups = "deployment")
    public void testBasicOperations() {
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setTenant(tenant);
        metadataSegmentExport.setType(ExportType.ACCOUNT);
        metadataSegmentExport.setSegment(segment);
        metadataSegmentExport.setStatus(Status.RUNNING);
        metadataSegmentExport.setPath("some/path");
        metadataSegmentExport.setCreatedBy(CREATED_BY);
        metadataSegmentExport.setCleanupBy(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));

        metadataSegmentExport = metadataSegmentExportService.createOrUpdateSegmentExportJob(metadataSegmentExport);

        Assert.assertNotNull(metadataSegmentExport.getPid());
        Assert.assertNotNull(metadataSegmentExport.getExportId());

        String exportId = metadataSegmentExport.getExportId();

        MetadataSegmentExport retrievedMetadataSegmentExport = metadataSegmentExportService
                .getSegmentExportByExportId(exportId);
        Assert.assertNotNull(retrievedMetadataSegmentExport);
        Assert.assertNotNull(retrievedMetadataSegmentExport.getPid());
        Assert.assertNotNull(retrievedMetadataSegmentExport.getExportId());

        metadataSegmentExportService.deleteSegmentExportByExportId(exportId);
    }
}
