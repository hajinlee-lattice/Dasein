package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.monitor.exposed.service.EmailService;

public abstract class IngestionProviderServiceImpl implements IngestionProviderService {

    private static final Logger log = LoggerFactory.getLogger(IngestionProviderServiceImpl.class);

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private EmailService emailService;

    protected boolean waitForFileToBeIngested(String destPath) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, destPath)) {
                    return true;
                }
            } catch (Exception e) {
                log.debug("Failed to check destination hdfs path {}, treat as not exist", destPath);
            }
            SleepUtils.sleep(1000L);
        }
        return false;
    }

    protected void emailNotify(ProviderConfiguration config, String ingestion, String version, String destPath) {
        if (!config.isEmailEnabled()) {
            return;
        }
        if (StringUtils.isBlank(config.getNotifyEmail())) {
            log.error("Email for notification is empty");
            return;
        }
        log.info(String.format("Sending notification email to %s", config.getNotifyEmail()));
        String subject = String.format("Ingestion %s for version %s is finished", ingestion, version);
        String content = String.format("Files are accessible at the following HDFS folder: %s", destPath);
        emailService.sendSimpleEmail(subject, content, "text/plain", Collections.singleton(config.getNotifyEmail()));
        log.info(String.format("Sent notification email to %s", config.getNotifyEmail()));
    }
}
