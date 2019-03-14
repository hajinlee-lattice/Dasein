package com.latticeengines.datacloud.workflow.match.steps;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CleanupBulkEntityMatchConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

/**
 * Cleanup step for bulk entity match workflow
 */
@Component("cleanupBulkEntityMatch")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CleanupBulkEntityMatch extends BaseWorkflowStep<CleanupBulkEntityMatchConfiguration> {

    private static final String OUTPUT_DIR = "Output";
    private static final String ERROR_DIR = "Error";

    private static Logger log = LoggerFactory.getLogger(CleanupBulkEntityMatch.class);

    @Inject
    private S3Service s3Service;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${datacloud.match.entity.test.result.s3.bucket}")
    private String s3Bucket;

    @Value("${datacloud.match.entity.test.result.s3.dir}")
    private String s3ResultDir;

    @Override
    public void execute() {
        copyResultToS3();
        cleanupTestDirectory();
    }

    /*
     * Copy bulk match result back to S3
     */
    private void copyResultToS3() {
        if (StringUtils.isBlank(configuration.getRootOperationUid())) {
            log.debug("Not bulk match performed, skip copying result files");
            // no bulk match performed
            return;
        }

        String uid = configuration.getRootOperationUid();
        HdfsPodContext.changeHdfsPodId(configuration.getPodId());
        String outputDir = hdfsPathBuilder.constructMatchOutputDir(uid).toString();
        String errorDir = hdfsPathBuilder.constructMatchErrorDir(uid).toString();
        log.debug("Match result directories. Output = {}, Error = {}", outputDir, errorDir);
        uploadFilesToS3(outputDir, baseResultPath(uid, OUTPUT_DIR));
        uploadFilesToS3(errorDir, baseResultPath(uid, ERROR_DIR));
    }

    /*
     * Upload all files under directory hdfsDir in hdfs (not recursive) to basePath
     * on S3
     */
    private void uploadFilesToS3(String hdfsDir, String basePath) {
        try {
            List<String> files = HdfsUtils.onlyGetFilesForDir(yarnConfiguration, hdfsDir, null);
            log.debug("Files = {} in {}", files, hdfsDir);
            for (String srcPath : files) {
                String filename = FilenameUtils.getName(srcPath);
                String dstPath = Paths.get(basePath, filename).toString();
                log.debug("Uploading file from HDFS({}) to S3({})", srcPath, dstPath);
                s3Service.uploadInputStream(s3Bucket, dstPath, HdfsUtils.getInputStream(yarnConfiguration, srcPath),
                        true);
            }
        } catch (Exception e) {
            log.error("Failed to upload files for directory {}, error = {}", hdfsDir, e);
        }
    }

    private String baseResultPath(@NotNull String uid, @NotNull String dir) {
        return Paths.get(s3ResultDir, uid, dir).toString();
    }

    private void cleanupTestDirectory() {
        String testDir = getConfiguration().getTmpDir();
        if (StringUtils.isNotBlank(testDir)) {
            log.info("Cleanup test directory ({}) for entity match", testDir);
            try {
                HdfsUtils.rmdir(yarnConfiguration, testDir);
            } catch (IOException e) {
                log.error("Failed to remove test directory ({}), error = {}", testDir, e);
            }
        }
    }
}
