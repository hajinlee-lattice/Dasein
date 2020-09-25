package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Date;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({GlobalAuthCleanupTestListener.class})
@TestExecutionListeners({DirtiesContextTestExecutionListener.class})
@ContextConfiguration(locations = {"classpath:test-pls-context.xml"})
public class MetadataSegmentExportServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Inject
    private MetadataSegmentExportService metadataSegmentExportService;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private AtlasExportProxy atlasExportProxy;

    @Inject
    private TestMetadataSegmentProxy testSegmentProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        attachProtectedProxy(testSegmentProxy);
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testBasicOperations() {
        String attributeSetName = UUID.randomUUID().toString();
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setType(AtlasExportType.ACCOUNT);
        metadataSegmentExport.setAccountFrontEndRestriction(new FrontEndRestriction());
        metadataSegmentExport.setContactFrontEndRestriction(new FrontEndRestriction());
        metadataSegmentExport.setStatus(Status.RUNNING);
        metadataSegmentExport.setExportPrefix(SEGMENT_NAME);
        metadataSegmentExport.setPath("some/path");
        metadataSegmentExport.setCreatedBy(CREATED_BY);
        metadataSegmentExport.setCleanupBy(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));
        metadataSegmentExport.setAttributeSetName(attributeSetName);
        metadataSegmentExport = testSegmentProxy.createSegmentExport(metadataSegmentExport);

        assertNotNull(metadataSegmentExport.getExportId());
        String exportId = metadataSegmentExport.getExportId();

        MetadataSegmentExport retrievedMetadataSegmentExport = metadataSegmentExportService.getSegmentExportByExportId(exportId);
        assertNotNull(retrievedMetadataSegmentExport);
        assertNotNull(retrievedMetadataSegmentExport.getExportId());
        assertNull(retrievedMetadataSegmentExport.getFileName());
        assertEquals(retrievedMetadataSegmentExport.getAttributeSetName(), attributeSetName);

        AtlasExport atlasExport = createAtlasExport(AtlasExportType.ACCOUNT, attributeSetName);
        metadataSegmentExport = metadataSegmentExportService.getSegmentExportByExportId(atlasExport.getUuid());
        assertNotNull(metadataSegmentExport.getExportId());
        assertEquals(metadataSegmentExportService.getSegmentExports().size(), 2);
        assertNotNull(metadataSegmentExport.getCreated());
        assertNotNull(metadataSegmentExport.getUpdated());
        assertEquals(metadataSegmentExport.getAttributeSetName(), attributeSetName);
    }

    private AtlasExport createAtlasExport(AtlasExportType atlasExportType, String attributeSetName) {
        AtlasExport atlasExport = new AtlasExport();
        atlasExport.setCreatedBy("default@lattice-engines.com");
        atlasExport.setAccountFrontEndRestriction(new FrontEndRestriction());
        atlasExport.setContactFrontEndRestriction(new FrontEndRestriction());
        atlasExport.setApplicationId(UUID.randomUUID().toString());
        atlasExport.setExportType(atlasExportType);
        atlasExport.setAttributeSetName(attributeSetName);
        atlasExport = atlasExportProxy.createAtlasExport(mainTestTenant.getId(), atlasExport);
        return atlasExport;
    }
}
