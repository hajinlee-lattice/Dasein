package com.latticeengines.dante.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.proxy.exposed.dante.DanteTalkingPointProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointResourceDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private final ObjectMapper objMapper = new ObjectMapper();

    @Autowired
    private DanteTalkingPointProxy danteTalkingPointProxy;

    @Autowired
    private TalkingPointEntityMgr talkingPointEntityMgr;

    private final String externalID = "talkingPointDepTestExtID";

    @BeforeClass(groups = "deployment")
    public void setup() {
        DanteTalkingPoint dtp = talkingPointEntityMgr.findByExternalID(externalID);
        if (dtp != null)
            talkingPointEntityMgr.delete(dtp);
    }

    @Test(groups = "deployment")
    public void testCreateFromService() {
        List<DanteTalkingPoint> dtps = new ArrayList<>();
        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID("test");
        dtp.setExternalID(externalID);
        dtp.setPlayExternalID("testDPlayExtID");
        dtp.setValue("Deployment Test Talking Point");
        dtps.add(dtp);

        ResponseDocument result = danteTalkingPointProxy.createOrUpdate(dtps);
        Assert.assertNull(result.getErrors());

        dtp = talkingPointEntityMgr.findByExternalID(dtp.getExternalID());

        Assert.assertNotNull(dtp.getCreationDate(), "Failure Cause: Creation Date is NULL");
        Assert.assertNotNull(dtp.getLastModificationDate(), "Failure Cause: LastModificationDate is NULL");

        Date oldLastModificationDate = dtp.getLastModificationDate();
        dtp.setValue("New Deployment Test Talking Point");
        dtps = new ArrayList<>();
        dtps.add(dtp);

        danteTalkingPointProxy.createOrUpdate(dtps);
        dtp = objMapper.convertValue(danteTalkingPointProxy.findByExternalID(externalID).getResult(),
                new TypeReference<DanteTalkingPoint>() {
                });

        Assert.assertNotNull(dtp,
                "Failure Cause: Talking Point not found by extrenal ID where externalID = " + externalID);
        Assert.assertEquals(dtp.getValue(), "New Deployment Test Talking Point",
                "Failure Cause: Talking Point value incorrect");

        Assert.assertNotEquals(dtp.getLastModificationDate(), oldLastModificationDate,
                "Failure Cause: Lastmodification date not updated by createOrUpdate()");

        dtps = objMapper.convertValue(danteTalkingPointProxy.findAllByPlayID("testDPlayExtID").getResult(),
                new TypeReference<List<DanteTalkingPoint>>() {
                });

        Assert.assertEquals(dtps.size(), 1, "Failure Cause: Talking Points not found by findByPlayID");

        danteTalkingPointProxy.delete(dtp.getExternalID());

        dtp = talkingPointEntityMgr.findByField("External_ID", externalID);
        Assert.assertNull(dtp, "Failure Cause: Talking point was not deleted");
    }

    @Test(groups = "deployment")
    public void testDanteOauth() {
        String testTenantName = "OauthTest.OauthTest.Production";
        ResponseDocument<DantePreviewResources> response = danteTalkingPointProxy.getPreviewResources(testTenantName);

        Assert.assertNotNull(response);
        Assert.assertNull(response.getErrors());
        Assert.assertNotNull(response.getResult());

        DantePreviewResources dpr = objMapper.convertValue(response.getResult(),
                new TypeReference<DantePreviewResources>() {
                });
        String url = dpr.getServerUrl() + "/tenants/oauthtotenant";
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + dpr.getoAuthToken());
        HttpEntity<String> entity = new HttpEntity<>("parameters", headers);

        String tenantNameViaToken = restTemplate.exchange(url, HttpMethod.GET, entity, String.class).getBody();
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, testTenantName);
    }
}
