package com.latticeengines.metadata.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.latticeengines.domain.exposed.serviceapps.cdl.MetadataJobType;
import com.latticeengines.metadata.service.MetadataMigrateDynamoService;
import com.latticeengines.metadata.service.MetadataTableCleanupService;
import com.latticeengines.metadata.service.impl.MetadataQuartzJobCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

public abstract class MetadataAbstractJobBean implements QuartzJobBean {

    private MetadataJobType metadataJobType;

    @Inject
    private MetadataTableCleanupService metadataTableCleanupService;

    @Inject
    private MetadataMigrateDynamoService metadataMigrateDynamoService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        MetadataQuartzJobCallable.Builder builder = new MetadataQuartzJobCallable.Builder();
        builder.metadataJobType(metadataJobType).metadataTableCleanupService(metadataTableCleanupService)
                .metadataMigrateDynamoService(metadataMigrateDynamoService).jobArguments(jobArguments);
        return new MetadataQuartzJobCallable(builder);
    }

    protected void setMetadataJobType(MetadataJobType metadataJobType) {
        this.metadataJobType = metadataJobType;
    }
}
