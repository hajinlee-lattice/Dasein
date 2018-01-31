package com.latticeengines.apps.cdl.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("cdlOrchestrationJob")
public class CDLOrchestrationJobBean extends CDLAbstractJobBean implements QuartzJobBean {
    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setCDLJobType(CDLJobType.PROCESSANALYZE);
        return super.getCallable(jobArguments);
    }
}
