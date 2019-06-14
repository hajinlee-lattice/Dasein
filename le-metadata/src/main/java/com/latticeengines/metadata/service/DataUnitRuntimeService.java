package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitRuntimeService {

    Boolean delete(DataUnit dataUnit);

    Boolean renameTableName(DataUnit dataUnit, String tableName);

}
