package com.latticeengines.metadata.service.impl;

import javax.annotation.PostConstruct;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.metadata.service.DataUnitRuntimeServiceRegistry;

abstract class AbstractDataUnitRuntimeServiceImpl<T extends DataUnit> implements DataUnitRuntimeService {

    @PostConstruct
    public void postConstruct() {
        DataUnitRuntimeServiceRegistry.register(getUnitClz(), this);
    }

    abstract Class<T> getUnitClz();

    public Boolean renameTableName(DataUnit dataUnit, String tableName) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support renaming.");
    }
}
