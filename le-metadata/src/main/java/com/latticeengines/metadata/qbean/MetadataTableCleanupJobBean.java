package com.latticeengines.metadata.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceapps.cdl.MetadataJobType;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("metadataTableCleanupJob")
public class MetadataTableCleanupJobBean extends MetadataAbstractJobBean implements QuartzJobBean {

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        setMetadataJobType(MetadataJobType.TABLE_CLEANUP);
        return super.getCallable(jobArguments);
    }
}
