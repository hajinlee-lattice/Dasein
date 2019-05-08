package com.latticeengines.pls.service.impl;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public class MetadataSegmentExportServiceImplDeploymentTestNG extends AbstractTestNGSpringContextTests {
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Autowired
    private MetadataSegmentExportService metadataSegmentExportService;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndData();

    }

    @Test(groups = "deployment")
    public void testBasicOperations() {
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setType(AtlasExportType.ACCOUNT);
        metadataSegmentExport.setAccountFrontEndRestriction(new FrontEndRestriction());
        metadataSegmentExport.setContactFrontEndRestriction(new FrontEndRestriction());
        metadataSegmentExport.setStatus(Status.RUNNING);
        metadataSegmentExport.setExportPrefix(SEGMENT_NAME);
        metadataSegmentExport.setPath("some/path");
        metadataSegmentExport.setCreatedBy(CREATED_BY);
        metadataSegmentExport.setCleanupBy(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));

        metadataSegmentExport = metadataSegmentExportService.createSegmentExportJob(metadataSegmentExport);

        Assert.assertNotNull(metadataSegmentExport.getPid());
        Assert.assertNotNull(metadataSegmentExport.getExportId());
        System.out.println("ExportId=" + metadataSegmentExport.getExportId());

        String exportId = metadataSegmentExport.getExportId();

        MetadataSegmentExport retrievedMetadataSegmentExport = metadataSegmentExportService
                .getSegmentExportByExportId(exportId);
        Assert.assertNotNull(retrievedMetadataSegmentExport);
        Assert.assertNotNull(retrievedMetadataSegmentExport.getPid());
        Assert.assertNotNull(retrievedMetadataSegmentExport.getExportId());
        Assert.assertTrue(retrievedMetadataSegmentExport.getFileName().startsWith(SEGMENT_NAME));

        metadataSegmentExportService.deleteSegmentExportByExportId(exportId);
    }
}
