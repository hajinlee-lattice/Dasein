package com.latticeengines.propdata.engine.ingestion.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.util.CronUtils;
import com.latticeengines.propdata.engine.ingestion.service.IngestionNewProgressValidator;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressService;
import com.latticeengines.propdata.engine.ingestion.service.IngestionService;
import com.latticeengines.propdata.engine.testframework.PropDataEngineFunctionalTestNGBase;

public class IngestionServiceImplFunctionalTestNG extends PropDataEngineFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(IngestionServiceImplFunctionalTestNG.class);

    private static final String INGESTION_NAME = IngestionNames.BOMBORA_FIREHOSE;
    private static final String HDFS_POD = "FunctionalBomboraFireHose";
    private static final String FILE_NAME = "Bombora_Firehose_20160422.csv.gz";
    private static final String CSV_GZ = "csv.gz";
    private static final String TEST_SUBMITTER = PropDataConstants.SCAN_SUBMITTER;

    @Autowired
    private IngestionService ingestionService;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private IngestionNewProgressValidator ingestionNewProgressValidator;

    private Ingestion ingestion;

    private IngestionProgress progress;

    private IngestionRequest ingestionRequest;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        ingestion = ingestionService.getIngestionByName(INGESTION_NAME);
        Assert.assertNotNull(ingestion);
        HdfsPodContext.changeHdfsPodId(HDFS_POD);
        ingestionRequest = createIngestionRequest();
        String destination = ingestionProgressService.constructDestination(ingestion,
                ingestionRequest.getFileName());
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Destination", destination);
        fields.put("Status", ProgressStatus.PROCESSING);
        ingestionProgressService.deleteProgressByField(fields);
    }

    @Test(groups = "functional", enabled = true)
    public void testIngestionInternal() throws JsonProcessingException {
        progress = ingestionService.ingestInternal(ingestion.getIngestionName(), ingestionRequest,
                HDFS_POD);
        Assert.assertNotNull(progress);
        log.info("New Ingestion Progress: " + progress.toString());
        Assert.assertNotNull(progress.getPid());
        Assert.assertEquals(progress.getStatus(), ProgressStatus.NEW);
        progress = ingestionProgressService.updateProgress(progress)
                .status(ProgressStatus.PROCESSING).commit(true);
        IngestionProgress duplicateProgress = ingestionService
                .ingestInternal(ingestion.getIngestionName(), ingestionRequest, HDFS_POD);
        Assert.assertNotNull(duplicateProgress);
        Assert.assertEquals(duplicateProgress.getStatus(), ProgressStatus.FAILED);
        log.info("Duplicate Ingestion Progress: " + duplicateProgress.toString());
        ingestionProgressService.deleteProgress(progress);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetMissingFiles() {
        List<String> missingFiles = ingestionService.getMissingFiles(ingestion);
        for (String file : missingFiles) {
            log.info("Missing File: " + file);
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testIsIngestionTriggered() {
        log.info("Ingestion is triggered: "
                + ingestionNewProgressValidator.isIngestionTriggered(ingestion));
    }

    @Test(groups = "functional", enabled = false)
    public void testHdfsUtils() throws IOException {
        List<String> files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration,
                "/Pods/TestIngestion", null);
        for (String file : files) {
            if (!HdfsUtils.isDirectory(yarnConfiguration, file) && file.endsWith(CSV_GZ)) {
                Path filePath = new Path(file);
                log.info(filePath.getName());
            }

        }
        com.latticeengines.domain.exposed.camille.Path hdfsDest = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName());
        log.info("Directory " + hdfsDest.toString() + " exists in HDFS: "
                + HdfsUtils.isDirectory(yarnConfiguration, hdfsDest.toString()));
    }

    @Test(groups = "functional", enabled = true)
    public void testCronUtils() throws ParseException {
        String cron = "0 24 23 * * ? *";
        Date previousFireTime = CronUtils.getPreviouFireTimeFromNext(cron);
        Assert.assertNotNull(previousFireTime);
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        log.info("Latest scheduled time: "
                + (previousFireTime == null ? "null" : df.format(previousFireTime))
                + " for ingestion " + ingestion.toString());
    }

    private IngestionRequest createIngestionRequest() {
        IngestionRequest request = new IngestionRequest();
        request.setFileName(FILE_NAME);
        request.setSubmitter(TEST_SUBMITTER);
        return request;
    }
}
