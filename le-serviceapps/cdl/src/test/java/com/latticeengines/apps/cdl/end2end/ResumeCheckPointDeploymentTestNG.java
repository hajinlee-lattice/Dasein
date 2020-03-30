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
import com.latticeengines.domain.exposed.cdl.End2EndTestConfiguration;
import com.latticeengines.domain.exposed.cdl.MockImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;

public class ResumeCheckPointDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ResumeCheckPointDeploymentTestNG.class);

    private static final String CHECK_POINT_CONFIG_FILE = "CHECK_POINT_CONFIG_FILE";
    protected static final String S3_CHECKPOINT_CONFIG_FILES_DIR = "le-serviceapps/cdl/end2end/checkpointConfigFiles/";
    protected static final String S3_BUCKET = "latticeengines-test-artifacts";
    protected static final String SUFFIX = ".json";

    @Inject
    private S3Service s3Service;

    private End2EndTestConfiguration end2EndTestConfiguration;

    @BeforeClass(groups = {"end2end"})
    @Override
    public void setup() throws Exception {
        readCheckpointConfigFile();
        if (end2EndTestConfiguration == null) {
            throw new IllegalArgumentException("can't read correct end2EndTestConfiguration file.");
        }
        setupEnd2EndTestEnvironment(end2EndTestConfiguration.getFeatureFlagMap());
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = {"end2end"})
    public void runtest() throws Exception {
        if (StringUtils.isNotEmpty(end2EndTestConfiguration.getResumeCheckpointName())) {
            log.info("resume checkpoint name is {}, version is {}.",
                    end2EndTestConfiguration.getResumeCheckpointName(), end2EndTestConfiguration.getResumeCheckpointVersion());
            resumeCheckpoint(end2EndTestConfiguration.getResumeCheckpointName(), end2EndTestConfiguration.getResumeCheckpointVersion());
        }
        importFile();
        if (end2EndTestConfiguration.isRunPA()) {
            if (isLocalEnvironment()) {
                processAnalyzeSkipPublishToS3();
            } else {
                processAnalyze(end2EndTestConfiguration.getProcessAnalyzeRequest());
            }
        }
        if (StringUtils.isNotEmpty(end2EndTestConfiguration.getCheckpointName())) {
            log.info("save checkpoint name is {}, version is {}.", end2EndTestConfiguration.getCheckpointName(),
                    end2EndTestConfiguration.getCheckpointVersion());
            saveCheckpoint(end2EndTestConfiguration.getCheckpointName(),
                    end2EndTestConfiguration.getCheckpointVersion(), end2EndTestConfiguration.isAuto());
        }
    }

    private void importFile() throws Exception {
        List<MockImport> mockImports = end2EndTestConfiguration.getImports();
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
        if (StringUtils.isBlank(System.getProperty(CHECK_POINT_CONFIG_FILE))) {
            throw new IllegalArgumentException("before run this test, we need set checkpointConfigFile at ENV");
        }
        String checkpointConfigFileName = System.getProperty(CHECK_POINT_CONFIG_FILE);
        String objectKey = S3_CHECKPOINT_CONFIG_FILES_DIR + addSuffix(checkpointConfigFileName);
        InputStream inputStream = s3Service.readObjectAsStream(S3_BUCKET, objectKey);
        end2EndTestConfiguration = JsonUtils.deserialize(inputStream, End2EndTestConfiguration.class);
    }

    private String addSuffix(String fileName) {
        if (fileName.endsWith(SUFFIX)) {
            return fileName;
        } else {
            return fileName + SUFFIX;
        }
    }
}
