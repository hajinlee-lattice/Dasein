package com.latticeengines.serviceflows.workflow.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.service.WorkflowDataUnitService;

@Service
public class WorkflowDataUnitServiceImpl implements WorkflowDataUnitService {

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private PrestoDbService prestoDbService;

    @Override
    public PrestoDataUnit registerPrestoDataUnit(HdfsDataUnit hdfsDataUnit) {

        return null;
    }

}
