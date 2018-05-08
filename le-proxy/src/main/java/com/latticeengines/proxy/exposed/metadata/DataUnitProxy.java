package com.latticeengines.proxy.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitProxy {

    DataUnit create(String customerSpace, DataUnit dataUnit);

    DataUnit getByNameAndType(String customerSpace, String name, DataUnit.StorageType type);

}
