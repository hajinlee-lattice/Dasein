package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionNewProgressValidator;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

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
        if (StringUtils.isNotBlank(ingestion.getCronExpression())) {
            return ingestionProgressEntityMgr.isIngestionTriggered(ingestion);
        }
        return true;
    }

    @Override
    public boolean isDuplicateProgress(IngestionProgress progress) {
        return ingestionProgressEntityMgr.isDuplicateProgress(progress);
    }

    @Override
    public List<IngestionProgress> cleanupDuplicateProgresses(List<IngestionProgress> progresses) {
        Iterator<IngestionProgress> iter = progresses.iterator();
        while (iter.hasNext()) {
            IngestionProgress progress = iter.next();
            if (isDuplicateProgress(progress)) {
                iter.remove();
                log.info("Duplicate progress is ignored: " + progress.toString());
            }
        }
        return progresses;
    }
}
