package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;

@Component("prestoDataUnitService")
public class PrestoDataUnitService extends AbstractDataUnitRuntimeServiceImpl<PrestoDataUnit> //
        implements DataUnitRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(PrestoDataUnitService.class);

    @Inject
    private PrestoDbService prestoDbService;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Override
    protected Class<PrestoDataUnit> getUnitClz() {
        return PrestoDataUnit.class;
    }

    @Override
    public Boolean delete(DataUnit dataUnit) {
        PrestoDataUnit prestoDataUnit = (PrestoDataUnit) dataUnit;
        String clusterId = prestoConnectionService.getClusterId();
        String tableName = prestoDataUnit.getPrestoTableName(clusterId);
        if (StringUtils.isNotBlank(tableName) && prestoDbService.tableExists(tableName)) {
            // FIXME: try to remove presto from other clusters
            log.info("Removing table {} from presto cluster {}", tableName, clusterId);
            prestoDbService.deleteTableIfExists(tableName);
        }
        return true;
    }

}
