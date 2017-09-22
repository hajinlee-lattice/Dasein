package com.latticeengines.apps.cdl.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

@Component("cdlJobService")
public class CDLJobServiceImpl implements CDLJobService {


    @Override
    public void submitJob(CDLJobType cdlJobType) {

    }
}
