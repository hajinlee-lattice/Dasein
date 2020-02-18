package com.latticeengines.metadata.qbean;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceapps.cdl.MetadataJobType;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("metadataMigrateDynamoJob")
public class MetadataMigrateDynamoJobBean extends MetadataAbstractJobBean implements QuartzJobBean {

    private static final Logger log = LoggerFactory.getLogger(MetadataMigrateDynamoJobBean.class);

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        setMetadataJobType(MetadataJobType.MIGRATE_DYNAMO);
        return super.getCallable(jobArguments);
    }
}
