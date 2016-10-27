package com.latticeengines.datacloud.etl.service;

import com.latticeengines.datacloud.core.source.Source;

public interface ServiceFlowsZkConfigService {

    boolean refreshJobEnabled(Source source);

    String refreshCronSchedule(Source source);

}
