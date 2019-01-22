package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.metadata.service.DataUnitService;

public class DynamoDataUnitService extends DataUnitRuntimeService<DynamoDataUnit> {

    private static final Logger log = LoggerFactory.getLogger(DynamoDataUnitService.class);

    @Inject
    private DynamoService dynamoService;

    @Override
    public Boolean delete(DynamoDataUnit dataUnit) {
        log.info("delete DynamoTable " + dataUnit.getName());
        dynamoService.deleteTable(dataUnit.getName());
        log.info("delete DynamoDataUnit record : tenant is " + dataUnit.getTenant() + ", name is " + dataUnit.getName());
        return true;
    }

    @Override
    public Boolean renameTableName(DynamoDataUnit dataUnit, String tablename) {
        throw new UnsupportedOperationException("DynamoDataUnitService can not support this method.");
    }
}
