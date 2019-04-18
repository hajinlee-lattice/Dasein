package com.latticeengines.pls.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.pls.service.MetadataSegmentExportCleanupService;
import com.latticeengines.pls.service.impl.MetadataSegmentExportCleanupCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("segmentExportCleanupJob")
public class SegmentExportCleanupJobBean implements QuartzJobBean {

    @Inject
    private MetadataSegmentExportCleanupService metadataSegmentExportCleanupService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        MetadataSegmentExportCleanupCallable.Builder builder = new MetadataSegmentExportCleanupCallable.Builder();
        builder.metadataSegmentExportCleanupService(metadataSegmentExportCleanupService);
        return new MetadataSegmentExportCleanupCallable(builder);
    }

}
