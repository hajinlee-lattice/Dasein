package com.latticeengines.apps.cdl.testframework;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Listeners;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public class CDLFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CDLFunctionalTestNGBase.class);

    protected static final String SEGMENT_NAME = "CDLTestSegment";

    @Resource(name = "globalAuthFunctionalTestBed")
    private GlobalAuthFunctionalTestBed testBed;

    @Inject
    private SegmentService segmentService;

    protected Tenant mainTestTenant;
    protected MetadataSegment testSegment;

    protected void setupTestEnvironmentWithDummySegment() {
        setupTestEnvironment();
        testSegment = createMetadataSegment(SEGMENT_NAME);
        log.info(String.format("Created metadata segment with name %s", testSegment.getName()));
    }

	protected MetadataSegment createMetadataSegment(String segmentName) {
		MetadataSegment metadataSegment = new MetadataSegment();
		metadataSegment.setDisplayName(segmentName);
        metadataSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(mainTestTenant.getId()).toString(), metadataSegment);
        MetadataSegment retrievedSegment = segmentService.findByName(CustomerSpace.parse(mainTestTenant.getId()).toString(),
        		metadataSegment.getName());
        Assert.assertNotNull(retrievedSegment);
		return retrievedSegment;
	}

    private void setupTestEnvironment() {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
    }
}
