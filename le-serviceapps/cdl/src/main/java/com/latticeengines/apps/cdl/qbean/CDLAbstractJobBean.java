package com.latticeengines.apps.cdl.qbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.impl.CDLQuartzJobCallable;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

public abstract class CDLAbstractJobBean implements QuartzJobBean {

    private CDLJobType cdlJobType;

    @Autowired
    private CDLJobService cdlJobService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        CDLQuartzJobCallable.Builder builder = new CDLQuartzJobCallable.Builder();
        builder.cdlJobType(cdlJobType)
                .cdlJobService(cdlJobService)
                .jobArguments(jobArguments);
        return new CDLQuartzJobCallable(builder);
    }

    protected void setCDLJobType(CDLJobType cdlJobType) {
        this.cdlJobType = cdlJobType;
    }
}
