package com.latticeengines.dante.controller;

import java.util.Date;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.DanteLeadEntityMgr;
import com.latticeengines.dante.testFramework.DanteTestNGBase;
import com.latticeengines.domain.exposed.dante.DanteLead;
import com.latticeengines.domain.exposed.dante.DanteLeadDTO;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteLeadResourceDeploymentTestNG extends DanteTestNGBase {
    @Autowired
    private DanteLeadProxy danteLeadProxy;

    @Autowired
    private DanteLeadEntityMgr danteLeadEntityMgr;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @BeforeClass(groups = "deployment")
    public void setup() {
        super.createDependences();
    }

    @Test(groups = "deployment")
    public void TestCreateRecommendation() {
        Recommendation testRec = getTestRecommendation();
        danteLeadProxy.create(new DanteLeadDTO(testRec, testPlay, testPlayLaunch), mainTestCustomerSpace.toString());

        DanteLead fromDante = danteLeadEntityMgr.findByField("externalID", testRec.getId());
        Assert.assertNotNull(fromDante);

        danteLeadEntityMgr.delete(fromDante);
    }

    private Recommendation getTestRecommendation() {
        Recommendation rec = new Recommendation();
        rec.setPid(12312L);
        rec.setAccountId("SomeAccountID");
        rec.setCompanyName("Some Company Name");
        rec.setRecommendationId(UUID.randomUUID().toString());
        rec.setDescription(testPlay.getDescription());
        rec.setLastUpdatedTimestamp(new Date());
        rec.setLaunchDate(new Date());
        rec.setLaunchId(testPlayLaunch.getLaunchId());
        rec.setLeAccountExternalID("SomeAccountID");
        rec.setLikelihood(null);
        rec.setMonetaryValue(null);
        rec.setPlayId(testPlay.getName());
        rec.setPriorityID(RuleBucketName.A);
        rec.setPriorityDisplayName("A");
        rec.setRecommendationId("10");
        rec.setSfdcAccountID("SomeAccountID");
        rec.setSfdcAccountID("SomeSFDCAccountID");
        rec.setTenantId(123L);
        return rec;
    }

    @AfterClass(groups = "deployment")
    public void cleanup() {
        super.deleteTestMetadataSegment();
    }

}
