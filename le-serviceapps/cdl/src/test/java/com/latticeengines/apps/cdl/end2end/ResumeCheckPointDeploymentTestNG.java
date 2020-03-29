package com.latticeengines.apps.cdl.end2end;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CheckpointConfiguration;
import com.latticeengines.domain.exposed.cdl.MockImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;

public class ResumeCheckPointDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ResumeCheckPointDeploymentTestNG.class);

    private static final String CHECK_POINT_CONFIG_FILE = "CHECK_POINT_CONFIG_FILE";
    protected static final String S3_CHECKPOINT_CONFIG_FILES_DIR = "le-serviceapps/cdl/end2end/checkpointConfigFiles/";
    protected static final String S3_BUCKET = "latticeengines-test-artifacts";

    @Inject
    private S3Service s3Service;

    private CheckpointConfiguration checkpointConfiguration;

    @BeforeClass(groups = {"end2end"})
    @Override
    public void setup() throws Exception {
        readCheckpointConfigFile();
        if (checkpointConfiguration == null) {
            throw new IllegalArgumentException("can't read correct checkpointConfiguration file.");
        }
        setupEnd2EndTestEnvironment(checkpointConfiguration.getFeatureFlagMap());
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = {"end2end"})
    public void runtest() throws Exception {
        if (StringUtils.isNotEmpty(checkpointConfiguration.getResumeCheckpointName())) {
            log.info("resume checkpoint name is {}, version is {}.",
                    checkpointConfiguration.getResumeCheckpointName(), checkpointConfiguration.getResumeCheckpointVersion());
            resumeCheckpoint(checkpointConfiguration.getResumeCheckpointName(), checkpointConfiguration.getResumeCheckpointVersion());
        }
        importFile();
        if (checkpointConfiguration.isRunPA()) {
            if (isLocalEnvironment()) {
                processAnalyzeSkipPublishToS3();
            } else {
                processAnalyze(checkpointConfiguration.getProcessAnalyzeRequest());
            }
        }
        if (StringUtils.isNotEmpty(checkpointConfiguration.getCheckpointName())) {
            log.info("save checkpoint name is {}, version is {}.", checkpointConfiguration.getCheckpointName(),
                    checkpointConfiguration.getCheckpointVersion());
            saveCheckpoint(checkpointConfiguration.getCheckpointName(),
                    checkpointConfiguration.getCheckpointVersion(), checkpointConfiguration.isAuto());
        }
    }

    private void importFile() throws Exception {
        List<MockImport> mockImports = checkpointConfiguration.getImports();
        if (CollectionUtils.isNotEmpty(mockImports)) {
            dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
            for (MockImport mockImport : mockImports) {
                EntityType entityType = EntityTypeUtils.matchFeedType(mockImport.getFeedType());
                if (entityType == null) {
                    throw new IllegalArgumentException(String.format("Can't find match entityType using feedType %s.",
                            mockImport.getFeedType()));
                }
                mockCSVImport(entityType.getEntity(), mockImport.getSuffix(), mockImport.getFileIdx(), mockImport.getFeedType());
                Thread.sleep(2000);
            }
            dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
        }
    }

    private void readCheckpointConfigFile() {
        if (StringUtils.isBlank(System.getenv(CHECK_POINT_CONFIG_FILE))) {
            throw new IllegalArgumentException("before run this test, we need set checkpointConfigFile at ENV");
        }
        String checkpointConfigFileName = System.getenv(CHECK_POINT_CONFIG_FILE);
        String objectKey = S3_CHECKPOINT_CONFIG_FILES_DIR + checkpointConfigFileName;
        InputStream inputStream = s3Service.readObjectAsStream(S3_BUCKET, objectKey);
        checkpointConfiguration = JsonUtils.deserialize(inputStream, CheckpointConfiguration.class);
    }
}
