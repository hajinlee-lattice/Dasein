package com.latticeengines.pls.controller.datacollection;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class MetadataSegmentResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final long ACCOUNTS_1 = 1171;
    private static final long CONTACTS_1 = 2956;
    private static final long ACCOUNTS_2 = 815;
    private static final long CONTACTS_2 = 1989;
    private static final long PRODUCTS = 149;


    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private TestMetadataSegmentProxy testSegmentProxy;

    private String segmentName;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        attachProtectedProxy(testSegmentProxy);
        cdlTestDataService.populateData(mainTestTenant.getId());
    }

    @Test(groups = "deployment")
    public void testCreate() {
        List<MetadataSegment> segments = testSegmentProxy.getSegments();
        Assert.assertEquals(segments.size(), 0);

        Date preCreationTime = new Date();

        MetadataSegment newSegment = new MetadataSegment();
        segmentName = NamingUtils.uuid("Segment");
        final String displayName = "Test Segment";
        final String description = "The description";
        newSegment.setName(segmentName);
        newSegment.setDisplayName(displayName);
        newSegment.setDescription(description);

        Restriction accountRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.LDC_Name.name()).gte("B").build();
        Restriction contactRestriction = Restriction.builder() //
                .let(BusinessEntity.Contact, InterfaceName.ContactName.name()).lt("R").build();
        newSegment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        newSegment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        MetadataSegment returned = testSegmentProxy.createOrUpdate(newSegment);
        Assert.assertNotNull(returned);
        Assert.assertEquals(returned.getName(), segmentName);
        Assert.assertEquals(returned.getDisplayName(), displayName);
        Assert.assertEquals(returned.getDescription(), description);

        Assert.assertNotNull(returned.getAccountFrontEndRestriction());
        Assert.assertNotNull(returned.getContactFrontEndRestriction());

        Assert.assertNotNull(returned.getCreated());
        Assert.assertTrue(preCreationTime.before(returned.getCreated()));
        Assert.assertNotNull(returned.getUpdated());
        Assert.assertTrue(preCreationTime.before(returned.getUpdated()));

        Assert.assertEquals(returned.getAccounts(), new Long(ACCOUNTS_1));
        Assert.assertEquals(returned.getContacts(), new Long(CONTACTS_1));
        Assert.assertEquals(returned.getProducts(), new Long(PRODUCTS));
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testUpdate() {
        MetadataSegment segment = testSegmentProxy.getSegment(segmentName);
        Assert.assertNotNull(segment);
        Assert.assertEquals(segment.getAccounts(), new Long(ACCOUNTS_1));
        Assert.assertEquals(segment.getContacts(), new Long(CONTACTS_1));
        Assert.assertEquals(segment.getProducts(), new Long(PRODUCTS));

        Restriction accountRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.LDC_Name.name()).gte("F").build();
        Restriction contactRestriction = Restriction.builder() //
                .let(BusinessEntity.Contact, InterfaceName.ContactName.name()).lt("N").build();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        Date preUpdateTime = new Date();
        MetadataSegment returned = testSegmentProxy.createOrUpdate(segment);

        Assert.assertNotNull(returned.getAccountFrontEndRestriction());
        Assert.assertNotNull(returned.getContactFrontEndRestriction());

        Assert.assertNotNull(returned.getCreated());
        Assert.assertTrue(preUpdateTime.after(returned.getCreated()));
        Assert.assertNotNull(returned.getUpdated());
        Assert.assertTrue(preUpdateTime.before(returned.getUpdated()));

        Assert.assertEquals(returned.getAccounts(), new Long(ACCOUNTS_2));
        Assert.assertEquals(returned.getContacts(), new Long(CONTACTS_2));
        Assert.assertEquals(returned.getProducts(), new Long(PRODUCTS));
    }

}
