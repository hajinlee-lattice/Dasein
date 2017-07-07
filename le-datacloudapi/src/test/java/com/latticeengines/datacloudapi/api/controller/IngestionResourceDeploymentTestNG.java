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
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionApiProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineProgress;
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

    @Autowired
    private IngestionApiProviderService apiProviderService;

    private static final String DNB_INGESTION = "DnBCacheSeedTest";
    private static final String DNB_VERSION = "2016-08-01_00-00-00_UTC";
    private static final String DNB_FILE = "LE_SEED_OUTPUT_2016_08_003.OUT.gz";
    private static final String ALEXA_INGESTION = "AlexaTest";
    private static final String ALEXA_VERSION_OLD = "2015-10-01_00-00-00_UTC";
    private static final String ALEXA_VERSION_NEW = "2015-11-01_00-00-00_UTC";
    private static final String ORB_INGESTION = "OrbTest";
    private String ORB_VERSION;
    private static final String SEMRUSH_INGESTION = "SemrushTest";
    private static final String SEMRUSH_VERSION = "2017-07-01_00-00-00_UTC";

    private int timeout = 2700000;

    // IngestionName, Config, IngestionType
    private static Object[][] getIngestions() {
        return new Object[][] {
                { DNB_INGESTION,
                        "{\"ClassName\":\"SftpConfiguration\",\"ConcurrentNum\":2,\"SftpHost\":\"10.41.1.31\",\"SftpPort\":22,\"SftpUsername\":\"sftpdev\",\"SftpPassword\":\"KPpl2JWz+k79LWvYIKz6cA==\",\"SftpDir\":\"/ingest_test/dnb\",\"CheckVersion\":1,\"CheckStrategy\":\"ALL\",\"FileExtension\":\"OUT.gz\",\"FileNamePrefix\":\"LE_SEED_OUTPUT_\",\"FileNamePostfix\":\"(.*)\",\"FileTimestamp\":\"yyyy_MM\"}",
                        IngestionType.SFTP }, //
                { ALEXA_INGESTION,
                        "{\"ClassName\":\"SqlToSourceConfiguration\",\"ConcurrentNum\":1,\"DbHost\":\"10.41.1.238\\\\\\\\SQL2012\",\"DbPort\":1437,\"Db\":\"CollectionDB_Dev\",\"DbUser\":\"DLTransfer\",\"DbPwdEncrypted\":\"Q1nh4HIYGkg4OnQIEbEuiw==\",\"DbTable\":\"Alexa\", \"Source\":\"Alexa\",\"TimestampColumn\":\"Creation_Date\",\"CollectCriteria\":\"NEW_DATA\",\"Mappers\":4}",
                        IngestionType.SQL_TO_SOURCE }, //
                { ORB_INGESTION,
                        "{\"ClassName\":\"ApiConfiguration\",\"ConcurrentNum\":1,\"VersionUrl\":\"http://api2.orb-intelligence.com/download/release-date.txt?api_key=54aebe74-0c2e-46d2-a8d7-086cd1ee8994\",\"VersionFormat\":\"EEE MMM dd HH:mm:ss 'UTC' yyyy\",\"FileUrl\":\"http://api2.orb-intelligence.com/download/orb-db2-export-sample.zip?api_key=54aebe74-0c2e-46d2-a8d7-086cd1ee8994\",\"FileName\":\"orb-db2-export-sample.zip\"}",
                        IngestionType.API }, //
                { SEMRUSH_INGESTION,
                        "{\"ClassName\":\"SqlToSourceConfiguration\",\"ConcurrentNum\":1,\"DbHost\":\"10.41.1.238\\\\\\\\SQL2012\",\"DbPort\":1437,\"Db\":\"CollectionDB_Dev\",\"DbUser\":\"DLTransfer\",\"DbPwdEncrypted\":\"Q1nh4HIYGkg4OnQIEbEuiw==\",\"DbTable\":\"Semrush_MostRecent\", \"Source\":\"SemrushMostRecent\",\"TimestampColumn\":\"LE_Last_Upload_Date\",\"CollectCriteria\":\"ALL_DATA\",\"Mappers\":4}",
                        IngestionType.SQL_TO_SOURCE }, //
        };
    }

    // IngestionName, ExpectedCreatedProgressNum, Version
    @DataProvider(name = "ExpectedResult")
    private static Object[][] getExpectedResult() {
        return new Object[][] { //
                { DNB_INGESTION, 3, DNB_VERSION, null }, //
                { ALEXA_INGESTION, 1, ALEXA_VERSION_NEW, 195 }, //
                { ORB_INGESTION, 1, null, null }, //
                { SEMRUSH_INGESTION, 1, SEMRUSH_VERSION, 10 }, //
        };
    };

    private List<Ingestion> ingestions = new ArrayList<>();

    @BeforeClass(groups = "deployment")
    public void init() throws Exception {
        prepareCleanPod(POD_ID);
        for (Object[] data : getIngestions()) {
            Ingestion ingestion = new Ingestion();
            ingestion.setIngestionName((String) data[0]);
            ingestion.setConfig((String) data[1]);
            ingestion.setSchedularEnabled(Boolean.TRUE);
            ingestion.setNewJobRetryInterval(10000L);
            ingestion.setNewJobMaxRetry(1);
            ingestion.setIngestionType((IngestionType) data[2]);
            ingestionEntityMgr.save(ingestion);
            ingestions.add(ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName()));
        }
        Ingestion alexaIngestion = ingestionEntityMgr.getIngestionByName(ALEXA_INGESTION);
        ingestionVersionService.updateCurrentVersion(alexaIngestion, ALEXA_VERSION_OLD);
        Ingestion orbIngestion = ingestionEntityMgr.getIngestionByName(ORB_INGESTION);
        ApiConfiguration apiConfig = (ApiConfiguration) orbIngestion.getProviderConfiguration();
        ORB_VERSION = apiProviderService.getTargetVersion(apiConfig);
    }

    @Test(groups = "deployment", priority = 1)
    public void testCreateDraftProgresses() {
        IngestionRequest request = new IngestionRequest();
        request.setSubmitter(PropDataConstants.SCAN_SUBMITTER);
        request.setSourceVersion(ALEXA_VERSION_NEW);
        IngestionProgress progress = ingestionProxy.ingestInternal(ALEXA_INGESTION, request, POD_ID);
        Assert.assertNotNull(progress);
        request = new IngestionRequest();
        request.setSubmitter(PropDataConstants.SCAN_SUBMITTER);
        request.setFileName(DNB_FILE);
        progress = ingestionProxy.ingestInternal(DNB_INGESTION, request, POD_ID);
        Assert.assertNotNull(progress);
        request = new IngestionRequest();
        request.setSubmitter(PropDataConstants.SCAN_SUBMITTER);
        request.setSourceVersion(SEMRUSH_VERSION);
        progress = ingestionProxy.ingestInternal(SEMRUSH_INGESTION, request, POD_ID);
        Assert.assertNotNull(progress);
        List<IngestionProgress> progresses = ingestionProxy.scan(POD_ID);
        Assert.assertTrue(CollectionUtils.isNotEmpty(progresses));
    }

    @Test(groups = "deployment", priority = 2, dataProvider = "ExpectedResult")
    public void testIngest(String name, int expectedProgresses, String version, Integer size) {
        if (name.equals(ORB_INGESTION)) {
            version = ORB_VERSION;
        }
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(name);
        Map<String, Object> fields = new HashMap<>();
        fields.put("IngestionId", ingestion.getPid());
        fields.put("Version", version);
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        Assert.assertEquals(progresses.size(), expectedProgresses);
        Long startTime = System.currentTimeMillis();
        EngineProgress engineProgress = ingestionVersionService.findProgressAtVersion(name, version);
        log.info(engineProgress);
        while (engineProgress.getStatus() != ProgressStatus.FINISHED
                && engineProgress.getStatus() != ProgressStatus.FAILED
                && System.currentTimeMillis() - startTime <= timeout) {
            ingestionProxy.scan(POD_ID);
            engineProgress = ingestionVersionService.findProgressAtVersion(name, version);
            log.info(engineProgress);
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
        }
        Assert.assertEquals(engineProgress.getStatus(), ProgressStatus.FINISHED);
        progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        for (IngestionProgress progress : progresses) {
            ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId, 3600);
            Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            Assert.assertEquals(progress.getStatus(), ProgressStatus.FINISHED);
            if (size != null) {
                Assert.assertEquals(progress.getSize().intValue(), size.intValue());
            }
            String currentVersion = ingestionVersionService.findCurrentVersion(ingestion);
            Assert.assertEquals(currentVersion, version);
        }
    }

    @AfterClass(groups = "deployment")
    public void destroy() throws Exception {
        for (Ingestion ingestion : ingestions) {
            ingestionEntityMgr.delete(ingestion);
        }
    }
}
