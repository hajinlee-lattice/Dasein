package com.latticeengines.apps.cdl.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("cdlExportScheduleJob")
public class CDLExportScheduleJobBean extends CDLAbstractJobBean implements QuartzJobBean {

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setCDLJobType(CDLJobType.EXPORT);
        return super.getCallable(jobArguments);
    }
}
