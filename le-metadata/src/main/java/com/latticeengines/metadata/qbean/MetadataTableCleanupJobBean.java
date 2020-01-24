package com.latticeengines.metadata.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.metadata.service.MetadataTableCleanupService;
import com.latticeengines.metadata.service.impl.MetadataTableCleanupJobCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("metadataTableCleanupJob")
public class MetadataTableCleanupJobBean implements QuartzJobBean {

    private static final Logger log = LoggerFactory.getLogger(MetadataTableCleanupJobBean.class);

    @Inject
    private MetadataTableCleanupService metadataTableCleanupService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        MetadataTableCleanupJobCallable.Builder builder = new MetadataTableCleanupJobCallable.Builder();
        builder.metadataTableCleanupService(metadataTableCleanupService).jobArguments(jobArguments);
        return new MetadataTableCleanupJobCallable(builder);
    }
}
