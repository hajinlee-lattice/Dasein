package com.latticeengines.propdata.core.service;

import com.latticeengines.propdata.core.source.Source;

public interface ServiceFlowsZkConfigService {

    boolean refreshJobEnabled(Source source);

    String refreshCronSchedule(Source source);

}
