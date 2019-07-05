package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("redshiftDataUnitService")
public class RedshiftDataUnitService extends AbstractDataUnitRuntimeServiceImpl<RedshiftDataUnit> //
        implements DataUnitRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(RedshiftDataUnitService.class);

    @Inject
    private RedshiftService redshiftService;

    @Override
    protected Class<RedshiftDataUnit> getUnitClz() {
        return RedshiftDataUnit.class;
    }

    @Override
    public Boolean delete(DataUnit dataUnit) {
        log.info("deleting RedshiftTable " + dataUnit.getName());
        redshiftService.dropTable(dataUnit.getName());
        log.info("deleted RedshiftDataUnit record : tenant is " + dataUnit.getTenant() //
                + ", name is " + dataUnit.getName());
        return true;
    }

    @Override
    public Boolean renameTableName(DataUnit dataUnit, String tableName) {
        String originTableName = dataUnit.getName();
        redshiftService.renameTable(originTableName, tableName);
        log.info("renamed RedShift tableName " + originTableName + " to " + tableName //
                + " under tenant " + dataUnit.getTenant());
        return true;
    }
}
