package com.latticeengines.propdata.collection.service;

import com.latticeengines.propdata.collection.source.Source;

public interface ZkConfigurationService {

    boolean refreshJobEnabled(Source source);

    String refreshCronSchedule(Source source);

}
