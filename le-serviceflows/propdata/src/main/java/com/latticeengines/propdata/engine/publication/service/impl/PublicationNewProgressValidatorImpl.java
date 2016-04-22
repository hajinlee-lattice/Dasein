package com.latticeengines.propdata.engine.publication.service.impl;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.CronUtils;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.engine.publication.service.PublicationNewProgressValidator;

@Component("publicationNewProgressValidator")
public class PublicationNewProgressValidatorImpl implements PublicationNewProgressValidator {

    private static final Log log = LogFactory.getLog(PublicationNewProgressValidatorImpl.class);

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

        if (noSourceAvro(publication.getSourceName(), currentVersion)) {
            return false;
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
            Date lastScheduledTime;
            try {
                lastScheduledTime = CronUtils.getPreviousFireTime(publication.getCronExpression());
            } catch (ParseException e) {
                throw new RuntimeException("Failed to parse cron expression " + publication.getCronExpression());
            }
            if (!lastTriggerTime.before(lastScheduledTime)) {
                // alread triggered
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

}
