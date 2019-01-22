package com.latticeengines.metadata.service.impl;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

public class HdfsDataUnitService extends DataUnitRuntimeService<HdfsDataUnit> {
    @Override
    public Boolean delete(HdfsDataUnit dataUnit) {
        throw new UnsupportedOperationException("HdfsDataUnitService can not support this method.");
    }

    @Override
    public Boolean renameTableName(HdfsDataUnit dataUnit, String tablename) {
        throw new UnsupportedOperationException("HdfsDataUnitService can not support this method.");
    }
}
