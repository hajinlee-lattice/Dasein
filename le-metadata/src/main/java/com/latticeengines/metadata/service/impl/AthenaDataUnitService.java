package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.prestodb.exposed.service.AthenaService;

@Component("athenaDataUnitService")
public class AthenaDataUnitService extends AbstractDataUnitRuntimeServiceImpl<AthenaDataUnit> //
        implements DataUnitRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(AthenaDataUnitService.class);

    @Inject
    private AthenaService athenaService;

    @Override
    protected Class<AthenaDataUnit> getUnitClz() {
        return AthenaDataUnit.class;
    }

    @Override
    public Boolean delete(DataUnit dataUnit) {
        AthenaDataUnit athenaDataUnit = (AthenaDataUnit) dataUnit;
        String tableName = athenaDataUnit.getAthenaTable();
        if (StringUtils.isNotBlank(tableName) && athenaService.tableExists(tableName)) {
            log.info("Removing table {} from Athena", tableName);
            athenaService.deleteTableIfExists(tableName);
        }
        return true;
    }

}
