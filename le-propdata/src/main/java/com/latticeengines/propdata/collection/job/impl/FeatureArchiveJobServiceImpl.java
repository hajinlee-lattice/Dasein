package com.latticeengines.propdata.collection.job.impl;

import org.quartz.DisallowConcurrentExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;
import com.latticeengines.propdata.collection.job.ArchiveJobService;
import com.latticeengines.propdata.collection.service.FeatureArchiveService;

@DisallowConcurrentExecution
@Component("featureArchiveJobService")
public class FeatureArchiveJobServiceImpl extends AbstractArchiveJobServiceImpl<FeatureArchiveProgress>
        implements ArchiveJobService {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private FeatureArchiveService featureArchiveService;

    @Override
    FeatureArchiveService getArchiveService() { return featureArchiveService; }

    @Override
    Logger getLogger() { return log; }

    @Override
    Class<FeatureArchiveProgress> getProgressClass() { return FeatureArchiveProgress.class; }

}
