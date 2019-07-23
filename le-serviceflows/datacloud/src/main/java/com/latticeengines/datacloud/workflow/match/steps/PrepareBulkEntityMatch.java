package com.latticeengines.datacloud.workflow.match.steps;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PrepareBulkEntityMatchConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

/**
 * Preparation step for bulk entity match workflow
 */
@Component("prepareBulkEntityMatch")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareBulkEntityMatch extends BaseWorkflowStep<PrepareBulkEntityMatchConfiguration> {

    private static Logger log = LoggerFactory.getLogger(PrepareBulkEntityMatch.class);

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Inject
    private S3Service s3Service;

    @Override
    public void execute() {
        log.info("Inside PrepareBulkEntityMatch execute()");

        bumpVersions(getConfiguration());
        copyTestFile(getConfiguration());
    }

    /*
     * copy test file (S3) to destination test directory (HDFS)
     */
    private void copyTestFile(@NotNull PrepareBulkEntityMatchConfiguration config) {
        if (!shouldCopyTestFile(config)) {
            log.info("Skip copy test file step");
            return;
        }

        // make sure dest dir is there
        String destDir = config.getDestTestDirectory();
        try {
            // cleanup dest directory
            if (HdfsUtils.fileExists(yarnConfiguration, destDir)) {
                HdfsUtils.rmdir(yarnConfiguration, destDir);
            }
            HdfsUtils.mkdir(yarnConfiguration, destDir);
        } catch (IOException e) {
            log.error("Failed to ensure destination dir {} is there, error = {}", destDir, e);
            throw new RuntimeException(e);
        }

        config.getSrcTestFilePath().forEach(path -> copy(destDir, path, config));
    }

    /*
     * copy test input file from S3 to HDFS
     */
    private void copy(@NotNull String destDir, @NotNull String srcPath,
            @NotNull PrepareBulkEntityMatchConfiguration config) {
        // TODO use distcp or other s3 connector if the file is too large
        try {
            InputStream is = s3Service.readObjectAsStream(config.getSrcBucket(), srcPath);
            // get the file name from source test file and use it as the file name in dest dir
            String destFilePath = FilenameUtils.concat(destDir, FilenameUtils.getName(srcPath));
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, destFilePath);
            log.info("Test file is copied from S3 (bucket={}, file={}) to dest ({})", config.getSrcBucket(), srcPath,
                    destFilePath);
        } catch (IOException e) {
            log.error("Failed to copy test file from S3 (bucket={}, file={}) to dest directory ({}), error = {}",
                    config.getSrcBucket(), config.getSrcTestFilePath(), config.getDestTestDirectory(), e);
            throw new RuntimeException(e);
        }
    }

    private boolean shouldCopyTestFile(@NotNull PrepareBulkEntityMatchConfiguration config) {
        return StringUtils.isNotBlank(config.getSrcBucket()) && CollectionUtils.isNotEmpty(config.getSrcTestFilePath())
                && StringUtils.isNotBlank(config.getDestTestDirectory());
    }

    /*
     * bump up version for specified environment/tenants
     */
    private void bumpVersions(@NotNull PrepareBulkEntityMatchConfiguration config) {
        Map<EntityMatchEnvironment, List<String>> customerSpaces = config.getCustomerSpacesToBumpUpVersion();
        if (MapUtils.isNotEmpty(customerSpaces)) {
            // bump up versions for tenants
            customerSpaces.entrySet().stream() //
                    .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue())) //
                    .forEach(entry -> {
                        EntityMatchEnvironment env = entry.getKey();
                        entry.getValue().stream().filter(Objects::nonNull).forEach(customerSpace -> {
                            // NOTE not checking whether tenant exists so we can bump up checkpoint version
                            Tenant tenant = new Tenant(customerSpace);
                            entityMatchVersionService.bumpVersion(env, tenant);
                        });
                    });
        } else {
            log.info("Skip bumping up version");
        }
    }
}
