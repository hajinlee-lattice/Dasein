package com.latticeengines.datacloudapi.api.controller;

import static com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType.BW_RAW;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.service.DataCloudEngineService;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.BWRawConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.proxy.exposed.datacloudapi.IngestionProxy;

public class IngestionBWRawDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    private final String POD_ID = "Default"; //this.getClass().getSimpleName();

    @Autowired
    private IngestionEntityMgr ingestionEntityMgr;

    @Autowired
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Autowired
    private IngestionProxy ingestionProxy;

    @Autowired
    private IngestionVersionService ingestionVersionService;

    @Resource(name = "ingestionVersionService")
    private DataCloudEngineService datacloudEngineService;

    @Autowired
    private IngestionProviderService ingestionBWRawProviderService;

    private static final String INGESTION = "BWRawTest";
    private static final String ING_VERSION = "2018-09-01_00-00-00_UTC";

    private int timeout = 2700000;

    private static Object[] getIngestions() {
        return new Object[]
                {
                        INGESTION,
                        "{\"ClassName\":\"BWRawConfiguration\",\"ConcurrentNum\":1}",
                        BW_RAW
                };
    }

    // IngestionName, ExpectedCreatedProgressNum, Version
    //@DataProvider(name = "ExpectedResult")
    private static Object[][] getExpectedResult() {
        return new Object[][] { //
                { INGESTION, 1, ING_VERSION, null }, //
       };
    }

    private List<Ingestion> ingestions = new ArrayList<>();

    //@BeforeClass(groups = "functional")
    public void init() throws Exception {
        //prepareCleanPod(POD_ID);
        Object[] ingestionData = getIngestions();
        Object[] data = ingestionData;
        Ingestion existing = ingestionEntityMgr.getIngestionByName((String) data[0]);
        if (existing != null) {
            ingestionEntityMgr.delete(existing);
        }

        Ingestion ingestion = new Ingestion();
        ingestion.setIngestionName((String) data[0]);
        ingestion.setConfig((String) data[1]);
        ingestion.setSchedularEnabled(Boolean.TRUE);
        ingestion.setNewJobRetryInterval(10000L);
        ingestion.setNewJobMaxRetry(1);
        ingestion.setIngestionType((Ingestion.IngestionType) data[2]);
        ingestionEntityMgr.save(ingestion);
        ingestions.add(ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName()));

        ingestion = ingestionEntityMgr.getIngestionByName(INGESTION);
        BWRawConfiguration config = (BWRawConfiguration)ingestion.getProviderConfiguration();
    }

    //@Test(groups = "functional", priority = 1)
    public void testCreateDraftProgresses() {
        IngestionRequest request = new IngestionRequest();
        request.setSubmitter(PropDataConstants.SCAN_SUBMITTER);
        request.setFileName("");
        request.setSourceVersion(ING_VERSION);
        IngestionProgress progress = ingestionProxy.ingestInternal(INGESTION, request, POD_ID);
        Assert.assertNotNull(progress);

        List<IngestionProgress> progresses = ingestionProxy.scan(POD_ID);
        Assert.assertTrue(CollectionUtils.isNotEmpty(progresses));
    }

    //@Test(groups = "functional", priority = 2, dataProvider = "ExpectedResult")
    public void testIngest(String name, int expectedProgresses, String version, Integer size) {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(name);
        Map<String, Object> fields = new HashMap<>();
        fields.put("IngestionId", ingestion.getPid());
        fields.put("Version", version);
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        Assert.assertEquals(progresses.size(), expectedProgresses);
        Long startTime = System.currentTimeMillis();
        ProgressStatus status = ingestionVersionService.findProgressAtVersion(name, version);
        while (status != ProgressStatus.FINISHED && status != ProgressStatus.FAILED
                && System.currentTimeMillis() - startTime <= timeout) {
            ingestionProxy.scan(POD_ID);
            status = ingestionVersionService.findProgressAtVersion(name, version);
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
        }
        Assert.assertEquals(status, ProgressStatus.FINISHED);
        progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        for (IngestionProgress progress : progresses) {
            ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
            FinalApplicationStatus appStatus = YarnUtils.waitFinalStatusForAppId(yarnClient, appId, 3600);
            Assert.assertEquals(appStatus, FinalApplicationStatus.SUCCEEDED);
            Assert.assertEquals(progress.getStatus(), ProgressStatus.FINISHED);
            if (size != null) {
                Assert.assertEquals(progress.getSize().intValue(), size.intValue());
            }
            String currentVersion = ingestionVersionService.findCurrentVersion(ingestion);
            Assert.assertEquals(currentVersion, version);
        }
    }

    //@AfterClass(groups = "functional")
    public void destroy() throws Exception {
        for (Ingestion ingestion : ingestions) {
            ingestionEntityMgr.delete(ingestion);
        }
    }

}
