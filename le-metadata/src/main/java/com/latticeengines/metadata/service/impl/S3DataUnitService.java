package com.latticeengines.metadata.service.impl;

import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

public class S3DataUnitService extends DataUnitRuntimeService<S3DataUnit> {
    @Override
    public Boolean delete(S3DataUnit dataUnit) {
        throw new UnsupportedOperationException("S3DataUnitService can not support this method.");
    }

    @Override
    public Boolean renameTableName(S3DataUnit dataUnit, String tablename) {
        throw new UnsupportedOperationException("S3DataUnitService can not support this method.");
    }
}
