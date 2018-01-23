package com.latticeengines.pls.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface CDLService {

    ApplicationId processAnalyze(String string);

}
