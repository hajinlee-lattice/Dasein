package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

@Component("dynamoDataUnitService")
public class DynamoDataUnitService extends AbstractDataUnitRuntimeServiceImpl<DynamoDataUnit> //
        implements DataUnitRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(DynamoDataUnitService.class);

    @Inject
    private DynamoService dynamoService;

    @Override
    protected Class<DynamoDataUnit> getUnitClz() {
        return DynamoDataUnit.class;
    }

    @Override
    public Boolean delete(DataUnit dataUnit) {
        log.info("deleting DynamoTable " + dataUnit.getName());
        dynamoService.deleteTable(dataUnit.getName());
        log.info("deleted DynamoDataUnit record : tenant is " + dataUnit.getTenant() //
                + ", name is " + dataUnit.getName());
        return true;
    }

}
