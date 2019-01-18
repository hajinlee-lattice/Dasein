package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("RedshiftDataUnitService")
public class RedshiftDataUnitService extends DataUnitRuntimeService<RedshiftDataUnit> {

    private static final Logger log = LoggerFactory.getLogger(RedshiftDataUnitService.class);

    @Inject
    private RedshiftService redshiftService;

    @Override
    public Boolean delete(RedshiftDataUnit dataUnit) {
        log.info("delete RedshiftTable " + dataUnit.getName());
        redshiftService.dropTable(dataUnit.getName());
        log.info("delete RedshiftDataUnit record : tenant is " + dataUnit.getTenant() + ", name is " + dataUnit.getName());
        return true;
    }

    @Override
    public Boolean renameTableName(RedshiftDataUnit dataUnit, String tablename) {
            String originTableName = dataUnit.getName();
            log.info("rename RedShift tableName " + originTableName + " to " + tablename + " under tenant " + dataUnit.getTenant());
            redshiftService.renameTable(originTableName, tablename);
            return true;
    }
}
