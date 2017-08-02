package com.latticeengines.dante.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.dante.testFramework.DanteTestNGBase;
import com.latticeengines.dante.testFramework.testDao.TestPlayDao;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;

public class TalkingPointResourceDeploymentTestNG extends DanteTestNGBase {
    @Autowired
    private TalkingPointProxy talkingPointProxy;

    @Autowired
    private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;

    @Autowired
    private TestPlayDao testPlayDao;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private Play testPlay;
    private static final String PLAY_DISPLAY_NAME = "DeplTestTPPlay";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @BeforeClass(groups = "deployment")
    public void setup() {
        testPlay = createTestPlay();
    }

    @Test(groups = "deployment")
    public void testCreateUpdate() {
        List<TalkingPointDTO> tps = new ArrayList<>();
        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setName("plsDeploymentTestTP1");
        tp.setPlayName(testPlay.getName());
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1");
        tps.add(tp);

        TalkingPointDTO tp1 = new TalkingPointDTO();
        tp1.setName("plsDeploymentTestTP2");
        tp1.setPlayName(testPlay.getName());
        tp1.setOffset(2);
        tp1.setTitle("Test TP2 Title");
        tp1.setContent("PLS Deployment Test Talking Point no 2");
        tps.add(tp1);

        List<TalkingPointDTO> rawcreate = talkingPointProxy.createOrUpdate(tps, mainTestCustomerSpace.toString());
        List<TalkingPointDTO> createResponse = JsonUtils.convertList(rawcreate, TalkingPointDTO.class);
        Assert.assertNotNull(createResponse);
        Assert.assertEquals(createResponse.size(), 2);

        List<TalkingPointDTO> raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
        List<TalkingPointDTO> playTpsResponse = JsonUtils.convertList(raw, TalkingPointDTO.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.size(), 2);
        Assert.assertEquals(playTpsResponse.get(0).getName(), createResponse.get(0).getName());
        Assert.assertEquals(playTpsResponse.get(1).getName(), createResponse.get(1).getName());

        playTpsResponse.get(0).setOffset(2);
        playTpsResponse.get(1).setOffset(1);

        rawcreate = talkingPointProxy.createOrUpdate(playTpsResponse, mainTestCustomerSpace.toString());
        createResponse = JsonUtils.convertList(rawcreate, TalkingPointDTO.class);
        Assert.assertNotNull(createResponse);
        Assert.assertNotEquals(tps.get(0).getName(), createResponse.get(0).getName());
        Assert.assertNotEquals(tps.get(1).getName(), createResponse.get(1).getName());

        raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
        playTpsResponse = JsonUtils.convertList(raw, TalkingPointDTO.class);
        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.size(), 2);
        Assert.assertEquals(playTpsResponse.get(0).getOffset(), 2);
        Assert.assertEquals(playTpsResponse.get(1).getOffset(), 1);
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" }, dependsOnMethods = { "testCreateUpdate" })
    public void testPreviewAndPublish() {
        List<TalkingPointDTO> raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
        List<TalkingPointDTO> tps = JsonUtils.convertList(raw, TalkingPointDTO.class);
        Assert.assertEquals(tps.size(), 2);

        // preview
        TalkingPointPreview tpPreview = talkingPointProxy.getTalkingPointPreview(testPlay.getName(),
                mainTestCustomerSpace.toString());
        Assert.assertNotNull(tpPreview);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 2);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(),
                tps.get(0).getName());

        // publish
        talkingPointProxy.publish(testPlay.getName(), mainTestCustomerSpace.toString());

        List<DanteTalkingPoint> dtps = danteTalkingPointEntityMgr.findAllByPlayID(testPlay.getName());
        Assert.assertEquals(dtps.size(), 2);

        Assert.assertEquals(dtps.get(0).getExternalID(), tps.get(0).getName());
        Assert.assertEquals(dtps.get(1).getExternalID(), tps.get(1).getName());

        // delete
        talkingPointProxy.delete(tps.get(0).getName());
        talkingPointProxy.delete(tps.get(1).getName());

        raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
        tps = JsonUtils.convertList(raw, TalkingPointDTO.class);
        Assert.assertEquals(tps.size(), 0);

        // preview
        tpPreview = talkingPointProxy.getTalkingPointPreview(testPlay.getName(), mainTestCustomerSpace.toString());
        Assert.assertNotNull(tpPreview);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 0);

        // publish
        talkingPointProxy.publish(testPlay.getName(), mainTestCustomerSpace.toString());

        dtps = danteTalkingPointEntityMgr.findAllByPlayID(testPlay.getName());
        Assert.assertEquals(dtps.size(), 0);
    }

    @Test(groups = "deployment")
    public void testDanteOauth() {
        DantePreviewResources previewResources = talkingPointProxy.getPreviewResources(mainTestTenant.getId());

        Assert.assertNotNull(previewResources);
        Assert.assertNotNull(previewResources.getDanteUrl());
        Assert.assertNotNull(previewResources.getoAuthToken());
        Assert.assertNotNull(previewResources.getServerUrl());

        String url = previewResources.getServerUrl() + "/playmaker/tenants/oauthtotenant";
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + previewResources.getoAuthToken());
        HttpEntity<String> entity = new HttpEntity<>("parameters", headers);

        String tenantNameViaToken = restTemplate.exchange(url, HttpMethod.GET, entity, String.class).getBody();
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, mainTestTenant.getId());
    }

    @AfterClass
    public void cleanup() {
        deletePlay(testPlay);
    }

    private void deletePlay(Play play) {
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayDao.delete(play);
            }
        });
    }

    private Play createTestPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);
        play.setTenant(mainTestTenant);
        play.setTenantId(mainTestTenant.getPid());
        play.setLastUpdatedTimestamp(new Date());
        play.setTimestamp(new Date());
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayDao.create(play);
            }
        });

        return play;
    }
}
