package com.latticeengines.propdata.engine.ingestion.service.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.propdata.engine.ingestion.service.IngestionNewProgressValidator;

@Component("ingestionNewProgressValidator")
public class IngestionNewProgressValidatorImpl implements IngestionNewProgressValidator {
    private static final Log log = LogFactory.getLog(IngestionNewProgressValidatorImpl.class);

    @Autowired
    IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Override
    public boolean isIngestionTriggered(Ingestion ingestion) {
        if (!ingestion.isSchedularEnabled()) {
            return false;
        }
        if (!StringUtils.isEmpty(ingestion.getCronExpression())) {
            return ingestionProgressEntityMgr.isIngestionTriggered(ingestion);
        }
        return true;
    }

    @Override
    public boolean isDuplicateProgress(IngestionProgress progress) {
        Map<String, Object> newFields = new HashMap<String, Object>();
        newFields.put("Destination", progress.getDestination());
        newFields.put("Status", ProgressStatus.NEW);
        Map<String, Object> processingFields = new HashMap<String, Object>();
        processingFields.put("Destination", progress.getDestination());
        processingFields.put("Status", ProgressStatus.PROCESSING);
        if (CollectionUtils.isEmpty(ingestionProgressEntityMgr.getProgressesByField(newFields))
                && CollectionUtils.isEmpty(
                        ingestionProgressEntityMgr.getProgressesByField(processingFields))) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public List<IngestionProgress> checkDuplcateProgresses(List<IngestionProgress> progresses) {
        Iterator<IngestionProgress> iter = progresses.iterator();
        while (iter.hasNext()) {
            IngestionProgress progress = iter.next();
            if (isDuplicateProgress(progress)) {
                iter.remove();
                log.info("Detect duplicate progress: " + progress.toString());
            }
        }
        return progresses;
    }
}
