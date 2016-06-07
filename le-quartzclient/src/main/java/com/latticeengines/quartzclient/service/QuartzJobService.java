package com.latticeengines.quartzclient.service;

import com.latticeengines.domain.exposed.quartz.PredefinedJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;

public interface QuartzJobService {

    TriggeredJobInfo runJob(PredefinedJobArguments jobArgs);

    Boolean hasActiveJob(PredefinedJobArguments jobArgs);

}
