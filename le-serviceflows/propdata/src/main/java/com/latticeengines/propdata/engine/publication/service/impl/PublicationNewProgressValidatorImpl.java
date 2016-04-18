package com.latticeengines.propdata.engine.publication.service.impl;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.core.util.CronUtils;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.engine.publication.service.PublicationNewProgressValidator;

@Component("publicationNewProgressValidator")
public class PublicationNewProgressValidatorImpl implements PublicationNewProgressValidator {

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Override
    public Boolean isValidToStartNewProgress(Publication publication, String currentVersion) {
        if (!publication.isSchedularEnabled()) {
            return false;
        }

        PublicationProgress existingProgress = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                currentVersion);
        if (existingProgress != null) {
            return false;
        }

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
                return false;
            }
        }

        return true;
    }

}
