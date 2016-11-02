package com.latticeengines.quartzclient.service;

import com.latticeengines.domain.exposed.quartz.QuartzJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;

public interface QuartzJobService {

    TriggeredJobInfo runJob(QuartzJobArguments jobArgs);

    Boolean hasActiveJob(QuartzJobArguments jobArgs);

    Boolean jobBeanExist(QuartzJobArguments jobArgs);

}
