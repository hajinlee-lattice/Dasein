package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationNewProgressValidator;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;

@Component("publicationNewProgressValidator")
public class PublicationNewProgressValidatorImpl implements PublicationNewProgressValidator {

    private static final Logger log = LoggerFactory.getLogger(PublicationNewProgressValidatorImpl.class);

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public Boolean isValidToStartNewProgress(Publication publication, String currentVersion) {
        if (!publication.isSchedularEnabled()) {
            return false;
        }

        if (hasExistingProgressAtVersion(publication, currentVersion)) {
            return false;
        }

        if (thisCronAlreadyTriggered(publication)) {
            return false;
        }

        switch (publication.getMaterialType()) {
            case SOURCE:
                if (noSourceAvro(publication.getSourceName(), currentVersion)) {
                    return false;
                }
                break;
            case INGESTION:
                if (noIngestionFile(publication.getSourceName(), currentVersion)) {
                    return false;
                }
        }


        return true;
    }

    private Boolean hasExistingProgressAtVersion(Publication publication, String currentVersion) {
        return progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication, currentVersion) != null;
    }

    private Boolean thisCronAlreadyTriggered(Publication publication) {
        if (StringUtils.isNotEmpty(publication.getCronExpression())) {
            PublicationProgress lastRun = progressEntityMgr.findLatestUnderMaximumRetry(publication);
            Date lastTriggerTime = lastRun.getCreateTime();
            Date lastScheduledTime = CronUtils.getPreviousFireTime(publication.getCronExpression()).toDate();
            if (!lastTriggerTime.before(lastScheduledTime)) {
                // already triggered
                return true;
            }
        }
        return false;
    }

    private Boolean noSourceAvro(String sourceName, String version) {
        Source source = sourceService.findBySourceName(sourceName);
        if (source instanceof DerivedSource) {
            String avroDir = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
            try {
                if (!HdfsUtils.fileExists(yarnConfiguration, avroDir)) {
                    log.warn("The avro dir " + avroDir + " does not exists.");
                    return true;
                }
                if (!HdfsUtils.fileExists(yarnConfiguration, avroDir + "/" + HdfsPathBuilder.SUCCESS_FILE)) {
                    log.warn("Cannot find the _SUCCESS file in the avro dir " + avroDir);
                    return true;
                }
                if (HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro").isEmpty()) {
                    log.warn("Cannot find any avro file in the avro dir " + avroDir);
                    return true;
                }
            } catch (Exception e) {
                log.warn("Failed to verify source avro for sourcename=" + sourceName + ", version=" + version);
            }
        } else {
            throw new UnsupportedOperationException("Only derived source can be published.");
        }
        return false;
    }

    private Boolean noIngestionFile(String ingestionName, String version) {
        String ingestionDir = hdfsPathBuilder.constructIngestionDir(ingestionName, version).toString();
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, ingestionDir)) {
                log.warn("The ingestion dir " + ingestionDir + " does not exists.");
                return true;
            }
            if (!HdfsUtils.fileExists(yarnConfiguration, ingestionDir + "/" + HdfsPathBuilder.SUCCESS_FILE)) {
                log.warn("Cannot find the _SUCCESS file in the ingestion dir " + ingestionDir);
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to verify file for ingestionname=" + ingestionName + ", version=" + version);
        }
        return false;
    }

}
