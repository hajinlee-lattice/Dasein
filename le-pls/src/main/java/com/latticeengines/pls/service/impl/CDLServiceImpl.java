package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.service.CDLService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

@Component("cdlService")
public class CDLServiceImpl implements CDLService {

    @Inject
    private CDLProxy cdlProxy;

    @Override
    public ApplicationId processAnalyze(String customerSpace) {
        return cdlProxy.processAnalyze(customerSpace, null);
    }
}
