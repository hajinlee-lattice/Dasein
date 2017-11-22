package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.monitor.exposed.service.EmailService;

public abstract class IngestionProviderServiceImpl implements IngestionProviderService {

    private static Logger log = LoggerFactory.getLogger(IngestionProviderServiceImpl.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private EmailService emailService;

    protected List<String> getHdfsFileNamesByExtension(String hdfsDir, final String fileExtension) {
        HdfsFilenameFilter filter = new HdfsFilenameFilter() {
            @Override
            public boolean accept(String filename) {
                return filename.endsWith(fileExtension);
            }
        };
        List<String> result = new ArrayList<String>();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, hdfsDir.toString())) {
                List<String> hdfsFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, filter);
                if (!CollectionUtils.isEmpty(hdfsFiles)) {
                    for (String fullName : hdfsFiles) {
                        result.add(new Path(fullName).getName());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to scan hdfs directory %s", hdfsDir.toString()), e);
        }
        return result;
    }

    protected boolean waitForFileToBeIngested(String destPath) {
        Long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, destPath)) {
                    return true;
                }
                Thread.sleep(1000L);
            } catch (Exception e) {
                // ignore
            }
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
