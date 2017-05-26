package com.latticeengines.datacloudapi.api.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.EngineProgress;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionCriteria;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.proxy.exposed.datacloudapi.IngestionProxy;

@Component
public class IngestionResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    public final String POD_ID = this.getClass().getSimpleName();

    private static Log log = LogFactory.getLog(IngestionResourceDeploymentTestNG.class);

    @Autowired
    private IngestionEntityMgr ingestionEntityMgr;

    @Autowired
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Autowired
    private IngestionProxy ingestionProxy;

    @Autowired
    private IngestionVersionService ingestionVersionService;

    private static final String DNB_VERSION = "2016-08-01_00-00-00_UTC";

    private int timeout = 1800000;

    // IngestionName, Config, SchedularEnabled, NewJobRetryInterval,
    // NewJobMaxRetry, IngestionType, IngestionCriteria,
    // ExpectedCreatedProgressNum
    @DataProvider(name = "ingestions")
    private static Object[][] getIngestions() {
        return new Object[][] {
 { "DnBCacheSeedTest",
                "{\"ClassName\":\"SftpConfiguration\",\"ConcurrentNum\":2,\"UncompressAfterIngestion\":0,\"SftpHost\":\"10.41.1.31\",\"SftpPort\":22,\"SftpUsername\":\"sftpdev\",\"SftpPassword\":\"KPpl2JWz+k79LWvYIKz6cA==\",\"SftpDir\":\"/ingest_test/dnb\",\"CheckVersion\":1,\"CheckStrategy\":\"ALL\",\"FileExtension\":\"OUT.gz\",\"FileNamePrefix\":\"LE_SEED_OUTPUT_\",\"FileNamePostfix\":\"(.*)\",\"FileTimestamp\":\"yyyy_MM\"}",
                IngestionType.SFTP, IngestionCriteria.ANY_MISSING_FILE, 3 }, //
        };
    }

    private List<Ingestion> ingestions = new ArrayList<>();

    @BeforeClass(groups = "deployment")
    public void init() throws Exception {
        for (Object[] data : getIngestions()) {
            Ingestion ingestion = new Ingestion();
            ingestion.setIngestionName((String) data[0]);
            ingestion.setConfig((String) data[1]);
            ingestion.setSchedularEnabled(Boolean.TRUE);
            ingestion.setNewJobRetryInterval(10000L);
            ingestion.setNewJobMaxRetry(3);
            ingestion.setIngestionType((IngestionType) data[2]);
            ingestion.setIngestionCriteria((IngestionCriteria) data[3]);
            ingestionEntityMgr.save(ingestion);
            ingestions.add(ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName()));
        }
    }

    @Test(groups = "deployment", priority = 1)
    public void testCreatePreprocessProgresses() {
        prepareCleanPod(POD_ID);
        List<IngestionProgress> progresses = ingestionProxy.scan(POD_ID);
        Assert.assertTrue(CollectionUtils.isNotEmpty(progresses));
    }

    @Test(groups = "deployment", priority = 2, dataProvider = "ingestions")
    public void testIngest(String name, String config, IngestionType type, IngestionCriteria criteria,
            int expectedProgresses) {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(name);
        Map<String, Object> fields = new HashMap<>();
        fields.put("IngestionId", ingestion.getPid());
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.getProgressesByField(fields, null);
        Assert.assertEquals(progresses.size(), expectedProgresses);
        Long startTime = System.currentTimeMillis();
        EngineProgress engineProgress = ingestionVersionService.status(name, DNB_VERSION);
        log.info(engineProgress);
        while (engineProgress.getStatus() != ProgressStatus.FINISHED
                && System.currentTimeMillis() - startTime <= timeout) {
            ingestionProxy.scan(POD_ID);
            engineProgress = ingestionVersionService.status(name, DNB_VERSION);
            log.info(engineProgress);
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
        }
        Assert.assertEquals(engineProgress.getStatus(), ProgressStatus.FINISHED);
        progresses = ingestionProgressEntityMgr.getProgressesByField(fields, null);
        for (IngestionProgress progress : progresses) {
            ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId, 3600);
            Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            Assert.assertEquals(progress.getStatus(), ProgressStatus.FINISHED);
        }
    }

    @AfterClass(groups = "deployment")
    public void destroy() throws Exception {
        for (Ingestion ingestion : ingestions) {
            ingestionEntityMgr.delete(ingestion);
        }
    }
}
